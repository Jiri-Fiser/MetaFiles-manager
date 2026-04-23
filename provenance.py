import copy
import glob
from collections import defaultdict
from pathlib import Path
from typing import Mapping, List, Tuple, Iterable, Union, MutableMapping
from urllib.parse import urlparse, ParseResult
from lxml.etree import QName

from ark import ArkIdentifier
from dict_tools import merge_maplists
from url_tools import is_absolute_http_url
from xml_conf import metadata_namespaces
from xml_tools import qname_to_clark, ElementLike, make_el, set_attr, clark_to_qname

import uuid

from typing import Optional, Tuple


DCTERMS_RELATIONS: List[Tuple[str, Optional[str]]] = [
    ("dcterms:hasPart", "dcterms:isPartOf"),
    ("dcterms:hasVersion", "dcterms:isVersionOf"),
    ("dcterms:hasFormat", "dcterms:isFormatOf"),
    ("dcterms:references", "dcterms:isReferencedBy"),
    ("dcterms:replaces", "dcterms:isReplacedBy"),
    ("dcterms:requires", "dcterms:isRequiredBy"),
    ("dcterms:relation", None),
    ("dcterms:source", None),
    ("dcterms:conformsTo", None),
]

def dcterms_with_inverse(tag: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (tag, inverse_tag) for a Dublin Core relation.

    If the given tag is the second element of a pair, the function
    still returns (tag, inverse). If the tag has no defined inverse,
    the second value is None.

    If the tag is not present in the relation list, returns (None, None).

    Examples
    --------
    >>> dcterms_with_inverse("dcterms:hasPart")
    ('dcterms:hasPart', 'dcterms:isPartOf')

    >>> dcterms_with_inverse("dcterms:isPartOf")
    ('dcterms:isPartOf', 'dcterms:hasPart')

    >>> dcterms_with_inverse("dcterms:relation")
    ('dcterms:relation', None)

    >>> dcterms_with_inverse("dcterms:title")
    (None, None)
    """
    for a, b in DCTERMS_RELATIONS:
        if tag == a:
            return a, b
        if tag == b:
            return b, a
    return None, None

def random_uri(prefix: str = "urn:uuid:") -> ParseResult:
    """
    Generate a random URI based on UUID4.

    Default form:
        urn:uuid:550e8400-e29b-41d4-a716-446655440000
    """
    return urlparse(f"{prefix}{uuid.uuid4()}")


def get_source(element: ElementLike) -> Tuple[bool, str]:
    source: List[Tuple[bool, str]] = []
    if "uri" in element.attrib:
        source.append((False, element.attrib["uri"]))
    if "pattern" in element.attrib:
        source.append((True, element.attrib["pattern"]))
    if "file" in element.attrib:
            source.append((True, glob.escape(element.attrib["file"])))
    if len(source) != 1:
        raise ValueError("Invalid activity source specification.")
    return source[0]

def ark_iterator(root_path: Path, target: Tuple[bool, str],
                 ark_map: Mapping[Path, ArkIdentifier])  -> Iterable[Union[Tuple[ArkIdentifier, Path],
                                                                           Tuple[ParseResult, None]]]:
    local, pattern = target
    if not local:
        yield urlparse(pattern), None
    else:
        files = Path(root_path).glob(pattern)
        for file in files:
            yield ark_map[file], file


def  process_data(child: ElementLike, root_path: Path, ark_map: Mapping[Path, ArkIdentifier],
                  outer_tag:str, inner_tag:str, entity_tag:str="prov:entity"
                  ) -> Iterable[Tuple[Union[ArkIdentifier, ParseResult], ElementLike]]:
    for target, path in ark_iterator(root_path, get_source(child), ark_map):
        used_elem = make_el(None, outer_tag, {}, metadata_namespaces)
        usage_elem = make_el(used_elem, inner_tag, {}, metadata_namespaces)
        target_str = target.url() if isinstance(target, ArkIdentifier) else target.geturl()
        make_el(usage_elem, entity_tag, {"rdf:resource": target_str}, metadata_namespaces)
        label = child.attrib["label"] if "label" in child.attrib else "data"
        make_el(usage_elem, "rdfs:label", {}, metadata_namespaces).text = label
        if path is not None:
            make_el(usage_elem, "dcterms:identifier", {}, metadata_namespaces).text = str(path)
        yield target, used_elem


def  process_agent(child: ElementLike, root_path: Path, ark_map: Mapping[Path, ArkIdentifier]
                   ) -> Iterable[Tuple[Union[ArkIdentifier, ParseResult], ElementLike]]:
    type = child.get("type", "person")
    qa_elem = make_el(None, "prov:qualifiedAssociation", {}, metadata_namespaces)
    a_elem = make_el(qa_elem, "prov:Association", {}, metadata_namespaces)
    agent_elem = make_el(a_elem, "prov:agent", {}, metadata_namespaces)
    uri = urlparse(child.attrib["uri"]).geturl() if "uri" in child.attrib else random_uri(prefix="urn:uuid:")

    if len(child):
        for subchild in child:
            agent_elem.append(copy.deepcopy(subchild))
    else:
        if type == "person":
            person_elem = make_el(agent_elem, "foaf:Person", {}, metadata_namespaces)
            if "uri" in child.attrib:
                set_attr(person_elem, "rdf:about", child.attrib["uri"], metadata_namespaces)
            else:
                make_el(person_elem, "foaf:name", {}, metadata_namespaces).text = child.text
        elif type == "software":
            sw_element = make_el(agent_elem, "prov:SoftwareAgent", {}, metadata_namespaces)
            if "uri" in child.attrib:
                set_attr(sw_element, "rdf:about", child.attrib["uri"], metadata_namespaces)
            else:
                make_el(sw_element, "dcterms:identifier", {}, metadata_namespaces).text = child.text
    label = child.attrib["label"] if "label" in child.attrib else "supervizor"
    make_el(a_elem, "rdfs:label", {}, metadata_namespaces).text = label
    yield  uri, qa_elem


def create_activity(target_arks: List[Union[ArkIdentifier, ParseResult]],
                    input_elements: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]],
                    agent_elements: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]],
                    metadata: List[ElementLike],
                    activity_id: str) -> Mapping[ArkIdentifier, List[ElementLike]]:
    output_elements: MutableMapping[ArkIdentifier, List[ElementLike]] = defaultdict(list)
    for target in target_arks:
        if not isinstance(target, ArkIdentifier):
            continue
        was_generated = make_el(None, "prov:wasGeneratedBy", {}, metadata_namespaces)
        activity = make_el(was_generated, "prov:Activity",
                           {"rdf:about": target.url() + activity_id}, metadata_namespaces)
        for elems in input_elements.values():
            for el in elems:
                activity.append(copy.deepcopy(el))
        for elems in agent_elements.values():
            for el in elems:
                activity.append(copy.deepcopy(el))
        for el in metadata:
            activity.append(copy.deepcopy(el))
        output_elements[target].append(was_generated)
    return output_elements


def create_backrefs(target: ArkIdentifier,
                    activity_id: str,
                    input_elements: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]],
                    agent_elements: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]]
                    ) -> Mapping[ArkIdentifier, List[ElementLike]]:
    backref_elements: MutableMapping[ArkIdentifier, List[ElementLike]] = defaultdict(list)
    for source in input_elements.keys():
        if not isinstance(source, ArkIdentifier):
            continue
        resource = target.url() + "#" + activity_id
        el = make_el(None, "prov:wasUsedBy", {"rdf:resource": resource}, metadata_namespaces)
        backref_elements[source].append(el)

    return backref_elements


def process_activity(el: ElementLike, root_path: Path,
                     ark_map: Mapping[Path, ArkIdentifier]) -> Mapping[ArkIdentifier, List[ElementLike]]:
    target_arks: List[Union[ArkIdentifier, ParseResult]] = []
    input_elems: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]] = defaultdict(list)
    agent_elems: MutableMapping[Union[ArkIdentifier, ParseResult], List[ElementLike]] = defaultdict(list)
    metadata: List[ElementLike] = []
    result: MutableMapping[ArkIdentifier, List[ElementLike]] = {}
    for child in el:
        if QName(child.tag).namespace == metadata_namespaces["mf"]:
            if QName(child.tag).localname == "input":
                for target, element in process_data(child, root_path, ark_map,
                                                    "prov:qualifiedUsage", "prov:Usage"):
                    input_elems[target].append(element)

            if QName(child.tag).localname == "agent":
                for target, element in process_agent(child, root_path, ark_map):
                    agent_elems[target].append(element)

            if QName(child.tag).localname == "output":
                for target, _ in ark_iterator(root_path, get_source(child), ark_map):
                    target_arks.append(target)

        if QName(child.tag).namespace == metadata_namespaces["dc"]:
            metadata.append(child)

    activity_id:str  = "#activity-" +  "_" + el.attrib["id"]

    target_ark_map = create_activity(target_arks, input_elems, agent_elems, metadata, activity_id)
    merge_maplists(result, target_ark_map)
    target_ark = [ark for ark in target_arks if isinstance(ark, ArkIdentifier)][0]
    backref_ark_map = create_backrefs(target_ark, activity_id, input_elems, agent_elems)
    merge_maplists(result, backref_ark_map)
    return result


def process_parallel_activity(el: ElementLike,
                              root_path: Path,
                              ark_map: Mapping[Path, ArkIdentifier]) -> Mapping[ArkIdentifier, List[ElementLike]]:
    raise NotImplementedError()

def process_dc_relation(dc_tag:str, about: str, resource:str, root_path:Path,
                        ark_map:  Mapping[Path, ArkIdentifier]) -> Mapping[ArkIdentifier, List[ElementLike]]:
    result = defaultdict(list)
    if dc_tag is None:
        return result
    local_about = is_absolute_http_url(about)
    local_resource = is_absolute_http_url(resource)
    for target_about, _ in ark_iterator(root_path, (local_about, about), ark_map):
        for resource_about, _ in ark_iterator(root_path, (local_resource, resource), ark_map):
            if isinstance(local_about, ArkIdentifier):
                element = make_el(None, dc_tag, {"rdf:resource": resource_about}, metadata_namespaces)
                result[local_about].append(element)
    return result

def process_provenance(element:ElementLike, root_path: Path,
                       ark_map: Mapping[Path, ArkIdentifier]) -> Mapping[ArkIdentifier, List[ElementLike]]:
    total_result = {}
    for el in element:
        dc_tag, dc_antitag = dcterms_with_inverse(clark_to_qname(el.tag, namespaces=metadata_namespaces))
        if el.tag == qname_to_clark("mf:activity", namespaces=metadata_namespaces):
            control_tag = el.xpath('./*[@regex]')
            if len(control_tag) == 0:
                result = process_activity(el, root_path, ark_map)
            elif len(control_tag) == 1:
                result = process_parallel_activity(el, root_path, ark_map)
            else:
                raise ValueError("Ambiguous regex pattern")
        elif dc_tag is not None:
            about = el.attrib[qname_to_clark("rdf:resource", namespaces=metadata_namespaces)]
            resource = el.attrib[qname_to_clark("rdf:resource", namespaces=metadata_namespaces)]
            result = {}
            result_direct = process_dc_relation(dc_tag, about, resource, root_path, ark_map)
            result_opposite = process_dc_relation(dc_antitag, resource, about, root_path, ark_map)
            merge_maplists(result, result_direct)
            merge_maplists(result, result_opposite)
        merge_maplists(total_result, result)
    return total_result