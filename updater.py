import json
import logging
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import Union, List, MutableMapping, Set, Mapping, Tuple, Dict, Iterable

from lxml import etree
from rdflib import Graph

from ark import ArkIdentifier
from db_storage import FileRecord, initialize_database, get_session
from dict_tools import merge_maplists, groupby
from filehash import hash_file,  hash_url, hash_str_betabet
from graph_store import canonical_graph_sha256
from metafiles_parser import substitute_attrs_by_elements, MetafilesParser, MFRule, ExternalSource
from policy import NameStrategy, get_localname
from provenance import process_provenance
from rdf_tools import fragments_to_rdf_graph
from xml_conf import metadata_namespaces
from matcher import FileMatcher
from xml_tools import substitute_placeholders_in_text_nodes, ElementLike, qname_to_clark, text_not_contains, split_elements_by_separator


def process_metafile(metafile: Union[Path, str], write_processed_metafile: bool = True) \
        -> Tuple[List[MFRule], List[Tuple[str,  ElementLike]], List[ExternalSource]]:
    metafile = Path(metafile)
    xml_parser = etree.XMLParser(remove_blank_text=True,
                                 resolve_entities=False,
                                 huge_tree=True,
                                 no_network=True,
                                 recover=False)

    tree = etree.parse(str(metafile), xml_parser)
    tree.xinclude()
    root = tree.getroot()
    attr_substitution_rules = {
        "project": ("mf:metadata.set", "mft:project", None),
        "prefix": ("mf:metadata.set", "mft:prefix", None),
        "manager": ("mf:metadata.set", "mft:manager", r"\s*,\s*"),
        "policy": ("mf:metadata.set", "mft:policy", None)
    }

    substitute_attrs_by_elements(
        root,
        xpath_expr="self::mf:dir | .//mf:dir | .//mf:files | .//mf:link",
        rules=attr_substitution_rules,
        namespaces=metadata_namespaces,
    )

    split_elements_by_separator(root, "mf:separator", namespaces=metadata_namespaces)

    if write_processed_metafile:
        tree.write( str(metafile.with_suffix(".out.xml")), encoding="utf-8", xml_declaration=True, pretty_print=True)

    parser = MetafilesParser(metadata_namespaces)
    parser.process_dir(root)
    return parser.rules, parser.provenances, parser.external_sources

def get_simple_value(tag: str, metadata: MutableMapping[str, Set[ElementLike]], namespaces: Mapping[str, str]) -> str:
    tag = qname_to_clark(tag, namespaces)
    if tag not in metadata:
        raise ValueError(f"unknown {tag} in metadata")
    elems = metadata[tag]
    if len(elems) != 1:
        raise ValueError(f"ambiguous {tag} in metadata")
    return next(iter(elems)).text # return text value of element

def get_arks(mf_rules: List[MFRule], data_path: Path, naan: str) -> Mapping[Path, ArkIdentifier]:
    file_to_ark: MutableMapping[Path, ArkIdentifier] = {}

    prefile_matcher = FileMatcher(mf_rules)
    for file, metadata, _ in prefile_matcher.process_subtree(data_path):
        if metadata:
            policy = json.loads(get_simple_value("mfterms:policy", metadata, metadata_namespaces))
            name_strategy = policy.get("local_name_strategy", NameStrategy.FILENAME_HASH_12)
            shoulder = get_simple_value("mfterms:prefix", metadata, metadata_namespaces)
            local = get_localname(file, data_path, name_strategy)
            ark = ArkIdentifier(naan, shoulder, local)
            file_to_ark[file] = ark

    return file_to_ark


def inject_provenance(metadata: MutableMapping[str, List[ElementLike]] , provenances: List[ElementLike]) -> None:
    merge_maplists(metadata, groupby(provenances, lambda elem: elem.tag))


def create_ark(file:Path, *, data_path:Path, naan:str, shoulder:str, policy: Dict) -> ArkIdentifier:
    name_strategy = policy.get("local_name_strategy", NameStrategy.FILENAME_HASH_12)
    local = get_localname(file, data_path, name_strategy)
    ark = ArkIdentifier(naan, shoulder, local)
    return ark

def init_session(database_uri):
    initialize_database(database_uri)
    return get_session(database_uri)

def get_provenances(provenances: List[Tuple[str, ElementLike]], data_path:Path, file_to_ark
                    ) -> Mapping[ArkIdentifier, List[ElementLike]]:
    result: MutableMapping[ArkIdentifier, List[ElementLike]] = {}
    for path, element in provenances:
        root_path = data_path / path
        partial_provenances = process_provenance(element, root_path, file_to_ark)
        merge_maplists(result, partial_provenances)
    return result


def create_rdf_graph(metadata, substitution, ark, remove_free: bool = False) -> Graph:
    g = None
    for key, elems in metadata.items():
        elems: Iterable[ElementLike] = (substitute_placeholders_in_text_nodes(elem, substitution) for elem in elems)
        if remove_free:
            elems = (elem for elem in elems if text_not_contains(elem, "{%"))
        g = fragments_to_rdf_graph(elems, str(ark), nsmap=metadata_namespaces, graph=g)
    return g


def update(naan:str, data_path:Path, metafile:Path, database_uri: str):
    mf_rules, provenance_elements, external_sources = process_metafile(metafile)
    file_to_ark: Mapping[Path, ArkIdentifier] = get_arks(mf_rules, data_path, naan)
    arks_provenances = get_provenances(provenance_elements, data_path, file_to_ark)

    session = init_session(database_uri)

    for file, metadata, substitution in  FileMatcher(mf_rules).process_subtree(data_path):
        if metadata:
            policy = json.loads(get_simple_value("mfterms:policy", metadata, metadata_namespaces))
            digest_data = hash_file(file, "sha256")
            substitution["hash"] = digest_data.hex()
            ark = create_ark(file, data_path=data_path,
                             naan=naan, shoulder= get_simple_value("mfterms:prefix", metadata, metadata_namespaces),
                             policy=policy)

            substitution["ark"] = str(ark)
            substitution["ark_url"] = ark.url()

            if ark in arks_provenances:
                inject_provenance(metadata, arks_provenances[ark])
            # save to database
            g = create_rdf_graph(metadata, substitution, ark)
            print(g.serialize(format="turtle"))
            record = FileRecord(ark=repr(ark),
                                storage=1,
                                local_path=str(file.relative_to(data_path)),
                                digest=digest_data,
                                digest_meta=canonical_graph_sha256(g),
                                created=datetime.now(), updated=datetime.now())
            record.upsert_with_policy(session=session, policy=policy["strictness"], graph=g)

    for external_source in external_sources:
        metadata = {key: set(elems) for key, elems in external_source.metadata.items()}
        policy = json.loads(get_simple_value("mfterms:policy", metadata, metadata_namespaces))
        ark = ArkIdentifier(naan=naan,
                            shoulder= get_simple_value("mfterms:prefix", metadata, metadata_namespaces),
                            locid=hash_str_betabet(external_source.local_ark, 12))
        substitution = dict(ark_url=ark.url(), ark=str(ark))
        g = create_rdf_graph(metadata, substitution, ark, remove_free=True)
        print(g.serialize(format="turtle"))
        record = FileRecord(ark=repr(ark),
                            storage=0,
                            local_path=external_source.url,
                            digest=hash_url(external_source.local_ark, 32),
                            digest_meta=canonical_graph_sha256(g),
                            created=datetime.now(), updated=datetime.now())
        record.upsert_with_policy(session=session, policy=policy["strictness"], graph=g)


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config_digitech.ini")
    ki_naan = config["Storage"]["Naan"]
    location = config[config["Storage"]["Location"]]
    dir_path = Path(location["Path"])
    metafiles_db = location["Database"]
    log = location["Log"]
    logging.basicConfig(filename=log, filemode="w", level=logging.INFO)


    update(ki_naan, dir_path, dir_path / "metafile.xml", metafiles_db)
