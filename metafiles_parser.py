from __future__ import annotations

import re

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from typing import Mapping, Tuple, MutableMapping, Optional, List, Iterable
from lxml import etree

from mfglob import glob_to_regex, canonicalize_path_pattern
from url_tools import is_absolute_http_url
from xml_tools import ElementLike, qname_to_clark, clark_to_qname

def compile_rules_to_clark(
    rules: Mapping[str, Tuple[str, str, Optional[str]]],
    namespaces: Optional[Mapping[str, str]] = None,
) -> Mapping[str, Tuple[str, str, str]]:
    """
    Pre-compile mapping from prefixed QNames to Clark-notation tags.
    """
    compiled: MutableMapping[str, Tuple[str, str, str]] = {}
    for attr_local, (existing_qn, new_qn, separator) in rules.items():
        if ":" in attr_local or attr_local.startswith("{"):
            raise ValueError(
                f"Rule key must be an unqualified attribute local-name, got: {attr_local!r}"
            )
        compiled[attr_local] = (
            qname_to_clark(existing_qn, namespaces),
            qname_to_clark(new_qn, namespaces),
            separator
        )
    return compiled

def has_short_single_line_text(elem, max_length: int) -> bool:
    text = elem.text
    return text is not None and len(elem) == 0 and "\n" not in text and "\r" not in text and len(text) <= max_length


def substitute_attrs_by_elements(
    root: ElementLike,
    xpath_expr: str,
    rules: Mapping[str, Tuple[str, str, Optional[str]]],
    *,
    namespaces: Mapping[str, str] = None,
) -> None:
    """
    For each element selected by XPath, replace selected unqualified attributes
    with new XML elements under an existing child element.

    Parameters
    ----------
    root:
        Root element of a lxml.etree tree (or any subtree root).
    xpath_expr:
        XPath expression selecting elements to process.
        Use `namespaces` if the XPath contains prefixes.
    rules:
        Mapping: attribute_local_name -> (existing_child_qname, new_child_qname, separator?)

        - `attribute_local_name` is *unqualified* (e.g., "who", not "mf:who").
        - `existing_child_qname` must name a child element that already exists
          under the processed element and will become the parent of the new element.
        - `new_child_qname` is the tag of the new element inserted under that child.
          The removed attribute value is stored as `new_element.text`.
        - `separator` is the separator between parts (None if value has no parts)
    namespaces:
        Optional prefix -> namespace URI mapping for XPath evaluation and target element specification.


    Notes
    -----
    - The function mutates the tree in place.
    - Attribute matching is strict for *unqualified* attributes:
      it matches only attributes with no namespace (key is plain "name").
    """
    rules = compile_rules_to_clark(rules, namespaces=namespaces)
    selected = root.xpath(xpath_expr, namespaces=namespaces or None)

    for el in selected:
        if not isinstance(el, etree._Element):
            continue

        # pro každý atribut, který má pravidlo
        for attr_local, (container_tag, new_child_tag, separator) in rules.items():
            if attr_local not in el.attrib:
                continue

            value = el.attrib.pop(attr_local)

            # najdi existující child rychle přes find (direct child)
            parent = el.find(container_tag)
            if parent is None:
                parent = etree.Element(container_tag)
                el.insert(0, parent)  # vlož jako první dítě

            if separator is not None:
                values = re.split(separator, value)
            else:
                values = [value]

            for pvalue in values:
                new_el = etree.SubElement(parent, new_child_tag)
                new_el.text = pvalue

@dataclass
class MFRule:
    pattern: str
    dir_pattern: str
    re_pattern: re.Pattern[str]
    metadata: Mapping[str, List[ElementLike]]

@dataclass(frozen=True)
class ExternalSource:
    url: str
    local_ark:str
    metadata: Mapping[str, List[ElementLike]]

class MetafilesParser:
    def __init__(self, namespaces: Mapping[str, str]) -> None:
        self.rules: List[MFRule] = []
        self.external_sources: List[ExternalSource] = []
        self.namespaces = namespaces
        self.provenances: List[Tuple[str,  ElementLike]] = []

    def process_dir(self, element: ElementLike, path=None, metadata=None,
                    namespace_filter: Optional[List[str]] = None) -> None:
        if path is None:
            path = []
            metadata = {}
        else:
            pattern = element.attrib["pattern"]
            assert pattern != "", "Empty pattern"
            path = path + [pattern]

        metadata = self.join_metadata(
            metadata,
            element.xpath("mf:metadata.set/*", namespaces=self.namespaces),
            element.xpath("mf:metadata.append/*", namespaces=self.namespaces),
            namespace_filter=namespace_filter)

        for element in element:
            if element.tag == f"{{{self.namespaces['mf']}}}dir":
                self.process_dir(element, path=path, metadata=metadata, namespace_filter=namespace_filter)
            if element.tag == f"{{{self.namespaces['mf']}}}files":
                self.process_files(element, path=path, metadata=metadata, namespace_filter=namespace_filter)
            if element.tag == f"{{{self.namespaces['mf']}}}link":
                self.process_link(element, metadata=metadata, namespace_filter=namespace_filter)
            if element.tag == f"{{{self.namespaces['mf']}}}provenance":
                self.provenances.append(("".join(path), element))


    def process_files(self, element: ElementLike,
                      path: List[str],
                      metadata,
                      namespace_filter: Optional[List[str]] = None) -> None:
        dir_pattern = canonicalize_path_pattern("/".join(path))
        path = path + [element.attrib["pattern"]]
        glob_pattern = canonicalize_path_pattern("/".join(path))

        metadata = self.join_metadata(
            metadata,
            element.xpath("mf:metadata.set/*", namespaces=self.namespaces),
            element.xpath("mf:metadata.append/*", namespaces=self.namespaces),
            namespace_filter=namespace_filter)

        re_pattern = re.compile(glob_to_regex(glob_pattern))
        self.rules.append(MFRule(pattern=glob_pattern,
                                 dir_pattern=dir_pattern,
                                 re_pattern=re_pattern,
                                 metadata=metadata))

    def process_link(self, element:ElementLike,
                     metadata,
                     namespace_filter: Optional[List[str]] = None) -> None:

        metadata = self.join_metadata(
            metadata,
            element.xpath("mf:metadata.set/*", namespaces=self.namespaces),
            element.xpath("mf:metadata.append/*", namespaces=self.namespaces),
            namespace_filter=namespace_filter)

        url = element.attrib["url"]
        if not is_absolute_http_url(url):
            raise Exception(f"Invalid url: {url}")

        local_ark = element.attrib["ark"]
        self.external_sources.append(ExternalSource(url=url, local_ark=local_ark, metadata=metadata))


    @staticmethod
    def join_metadata(metadata: MutableMapping[str, List[ElementLike]],
                      set_elements: Iterable[ElementLike],
                      add_elements: Iterable[ElementLike],
                      namespace_filter: Optional[List[str]] = None,
                      ) -> MutableMapping[str, List[ElementLike]]:
        result = defaultdict(list)
        for element in set_elements:
            if namespace_filter and etree.QName(element).namespace not in namespace_filter:
                continue
            result[element.tag].append(element)
        for key, value in metadata.items():
            if key not in result:
                result[key] = list(value)
        for element in add_elements:
            if namespace_filter and etree.QName(element).namespace not in namespace_filter:
                continue
            result[element.tag].append(element)
        return result

    @staticmethod
    def print_element_dict(data: Mapping[str, List[ElementLike]], namespaces: Mapping[str, str], indent: int =4) -> None:
        """
        Pretty-print a dictionary mapping element names to lists of lxml etree elements.

        Each dictionary key is printed on its own line, followed by the associated
        elements rendered as single-line XML strings.
        :param data: dictionary mapping element names to lists
        :param namespaces: dictionary of namespace prefixes
        :param indent: indent of printing
        """
        for key, elements in data.items():
            print(" " * indent + f"{clark_to_qname(key, namespaces)}:")
            for el in elements:
                if has_short_single_line_text(el, 120):
                    print(" "*indent + f"   {el.text}")
                    continue
                tmp = deepcopy(el)
                # lxml: etree.Element is a C-level factory; it does not match the Protocol type (false positive)
                # noinspection PyTypeChecker
                wrapper: ElementLike = etree.Element("wrapper", nsmap=namespaces)
                wrapper.append(tmp)
                xml = etree.tostring(tmp, method="c14n", exclusive=True).decode("utf-8")
                print(" "*indent + f"   {xml}")








