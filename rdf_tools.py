from typing import Mapping

from rdflib import Graph, URIRef, Literal
from typing import Iterable, Optional, Union, Dict
from lxml import etree
import copy

from xml_tools import ElementLike

RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XML_NS = "http://www.w3.org/XML/1998/namespace"

def _bind_prefixes(g: Graph, nsmap: Mapping[str, str]) -> None:
    for prefix, ns in nsmap.items():
        g.bind(prefix, ns, override=True)

def _wrap_property_fragment_as_rdfxml(
    subject_iri: str,
    prop_elem: etree._Element,
    nsmap: Dict[str, str],
) -> bytes:
    """
    Wrap a property element into a minimal RDF/XML document,
    using ONLY the explicitly provided namespace map.
    """
    rdf_ns = nsmap.get("rdf")
    if rdf_ns is None:
        raise ValueError("nsmap must contain 'rdf' namespace")

    rdf_RDF = etree.Element(etree.QName(rdf_ns, "RDF"), nsmap=nsmap)
    desc = etree.SubElement(rdf_RDF, etree.QName(rdf_ns, "Description"))
    desc.set(etree.QName(rdf_ns, "about"), subject_iri)

    # Important: deep copy, original XML stays untouched
    desc.append(copy.deepcopy(prop_elem))

    return etree.tostring(
        rdf_RDF,
        encoding="utf-8",
        xml_declaration=True,
    )



def fragments_to_rdf_graph(
    fragments: Iterable[ElementLike],
    subject: Union[str, URIRef],
    *,
    nsmap: Dict[str, str],
    graph: Optional[Graph] = None,
) -> Graph:
    """
    Build an RDF graph from XML fragments using an explicit namespace whitelist.
    """
    if graph is None:
        g = Graph()
        _bind_prefixes(g, nsmap)
    else:
        g = graph

    subj_iri = str(subject) if isinstance(subject, URIRef) else subject
    subj = URIRef(subj_iri)

    for el in fragments:
        has_children = len(el) > 0
        has_rdfxml_attrs = (
            el.get(etree.QName(RDF_NS, "parseType")) is not None
            or el.get(etree.QName(RDF_NS, "nodeID")) is not None
        )

        # 1) Try RDF/XML route for complex fragments
        if has_children or has_rdfxml_attrs:
            rdfxml = _wrap_property_fragment_as_rdfxml(subj_iri, el, nsmap)
            try:
                g.parse(data=rdfxml, format="application/rdf+xml")
                continue
            except Exception:
                # fall through to simple handling
                pass

        # 2) Simple triple handling
        qn = etree.QName(el)
        if not qn.namespace:
            raise ValueError(f"Element {qn.localname} has no namespace")

        pred = URIRef(qn.namespace + qn.localname)

        # rdf:resource → IRI object
        res = el.get(etree.QName(RDF_NS, "resource"))
        if res is not None:
            g.add((subj, pred, URIRef(res)))
            continue

        # literal object
        text = el.text or ""
        lang = el.get(etree.QName(XML_NS, "lang"))
        dtype = el.get(etree.QName(RDF_NS, "datatype"))

        if dtype is not None:
            obj = Literal(text, datatype=URIRef(dtype))
        elif lang is not None:
            obj = Literal(text, lang=lang)
        else:
            obj = Literal(text)

        g.add((subj, pred, obj))

    for s, p, o in list(g.triples((None, URIRef("id"), None))):
        g.remove((s, p, o))
    return g
