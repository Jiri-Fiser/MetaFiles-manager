from __future__ import annotations

from copy import deepcopy
from typing import Protocol, MutableMapping, Optional, Mapping, Iterator, Any, Union
import re
from typing import Mapping, Optional
from xml.etree.ElementTree import QName

import lxml.etree as etree

class ElementLike(Protocol):
    tag: str
    text: str
    attrib: MutableMapping[str, str]

    def append(self, element: "ElementLike") -> None: ...
    def set(self, key: Union[str, QName], value: str) -> None: ...
    def get(self, key: Union[str, QName], default: Optional[str] = None) -> Optional[str]: ...
    def xpath(self, path: str, namespaces: Optional[Mapping[str, str]] = None) -> Any: ...
    def __iter__(self) -> Iterator["ElementLike"]: ...
    def __len__(self) -> int: ...
    def getparent(self) -> Optional["ElementLike"]: ...
    def remove(self, element: "ElementLike") -> None: ...
    def iterdescendants(self) -> Iterator["ElementLike"]: ...
    def itertext(self) -> Iterator[str]: ...

def substitute_placeholders_in_text_nodes(
    root: ElementLike,
    values: Mapping[str, str],
    *,
    strict: bool = False,
) -> ElementLike:
    """
    Substitute placeholders like '{%key%}' in all text nodes (.text and .tail)
    within the copy of subtree rooted at `root`.

    Args:
        root: Root element of the subtree to process.
        values: Mapping key -> replacement (will be converted to str).
        strict: If True, raise KeyError when placeholder key is not in `values`.
                If False, leave unknown placeholders unchanged.

    Modifies the tree in-place. Returns None.
    """
    # Pre-stringify values once (faster, and ensures deterministic output)

    _PLACEHOLDER_RE = re.compile(r"\{%([A-Za-z0-9_.:-]+)%\}")

    def repl(match: re.Match) -> str:
        key = match.group(1)
        if key in values:
            return values[key]
        if strict:
            raise KeyError(f"Missing placeholder key: {key!r}")
        return match.group(0)  # keep as-is

    def sub(s: Optional[str]) -> Optional[str]:
        if s is None or "{%" not in s:
            return s
        return _PLACEHOLDER_RE.sub(repl, s)

    root = deepcopy(root)

    # all descendants (includes root again, but cheap to handle explicitly)
    for el in root.iter():
        el.text = sub(el.text)
        el.tail = sub(el.tail)

        for attr_name, attr_value in el.attrib.items():
            el.attrib[attr_name] = sub(attr_value)

    return root

def text_not_contains(element: ElementLike, needle: str) -> bool:
    """
    Return True if no text node in `element` or its descendants
    contains `needle`.

    Examples:
        >>> from lxml import etree
        >>> root = etree.fromstring("<p>Ahoj <b>světe</b>!</p>")
        >>> text_not_contains(root, "xxx")
        True
        >>> text_not_contains(root, "svě")
        False
    """
    return all(needle not in text for text in element.itertext())

def qname_to_clark(qname: str, namespaces: Mapping[str, str]) -> str:
    """
    Convert 'prefix:local' (or 'local', or '{uri}local') to Clark notation '{uri}local'.
    """
    if qname.startswith("{"):
        return qname  # already Clark
    if ":" not in qname:
        return qname  # unnamespaced
    prefix, local = qname.split(":", 1)
    try:
        uri = namespaces[prefix]
    except KeyError as e:
        raise ValueError(f"Unknown prefix '{prefix}' in QName '{qname}'.") from e
    return f"{{{uri}}}{local}"


def clark_to_qname(clark: str, namespaces: Mapping[str, str]) -> str:
    """
    Convert Clark notation '{uri}local' (or 'local', or 'prefix:local')
    to 'prefix:local' if possible, otherwise to 'local'.
    """
    # already QName or unnamespaced
    if not clark.startswith("{"):
        return clark

    try:
        uri, local = clark[1:].split("}", 1)
    except ValueError as e:
        raise ValueError(f"Invalid Clark notation '{clark}'.") from e

    # find matching prefix
    for prefix, ns_uri in namespaces.items():
        if ns_uri == uri:
            return f"{prefix}:{local}"

    # namespace not known → drop prefix
    return local


def make_el(
    parent: Optional[ElementLike],
    qname: str,
    attrs: Optional[Mapping[str, str]],
    namespaces: Mapping[Optional[str], str],
) -> ElementLike:
    """
    Create an lxml etree element with namespace support.

    Parameters
    ----------
    parent:
        Parent element or None (root element).
    qname:
        Element name in one of the forms:
        - "local"
        - "prefix:local"
        - "{uri}local"
    attrs:
        Attribute mapping. Keys may be:
        - "attr"
        - "prefix:attr"
        - "{uri}attr"
    namespaces:
        Mapping of namespace prefixes to URIs.
        Use key None for default namespace.

    Returns
    -------
    etree._Element
    """

    tag = qname_to_clark(qname, namespaces)

    attrib = {}
    if attrs:
        for k, v in attrs.items():
            attrib[qname_to_clark(k, namespaces)] = v

    if parent is None:
        nsmap = {k: v for k, v in namespaces.items() if v is not None}
        return etree.Element(tag, nsmap=nsmap if nsmap else None, attrib=attrib)

    return etree.SubElement(parent, tag, attrib=attrib)


def set_attr(
    elem: ElementLike,
    name: str,
    value: str,
    namespaces: Mapping[str, str],
) -> None:
    """
    Set an attribute on an lxml element, where the attribute name may be:

    - Clark notation: "{uri}local"
    - Prefixed QName: "prefix:local"  (resolved via `namespaces`)
    - Unnamespaced:  "local"

    The `qname_to_clark(name, namespaces)` helper is used to normalize the name.
    """
    clark = qname_to_clark(name, namespaces)

    if not isinstance(value, str):
        raise TypeError(f"Attribute value must be str, got {type(value).__name__}.")

    elem.set(clark, value)



def split_elements_by_separator(root: ElementLike,
                                separator_attr:str,
                                namespaces: Mapping[Optional[str], str]) -> None:
    """
    Modify an lxml tree in-place by replacing elements that have the
    `attrib` attribute with multiple sibling
    elements of the same tag.

    The original element is split into multiple elements by dividing its
    *text content* using the separator specified in the attribute value.
    Surrounding whitespace around the separator is ignored, i.e. splitting
    is performed using the pattern:

        \\s*<separator>\\s*

    Example:
        <a mf:separator=",">x, y ,z</a>

    becomes:

        <a>x</a>
        <a>y</a>
        <a>z</a>

    Properties preserved:
    - Element tag (including namespace)
    - All attributes except the separator attribute
    - Tail text (assigned to the last generated element)

    Limitations:
    - The function assumes that the element contains only text (no child elements).
      If child elements are present, a ValueError is raised.
    - The root element cannot be split into multiple siblings; if it has the
      separator attribute, a ValueError is raised.

    Args:
        root: An lxml Element to be modified in-place.
        separator_attr: The qname of attribute used to split the element's text.
        namespaces: Mapping of namespace prefixes to URIs.

    Raises:
        ValueError: If a split is attempted on an element with children or on the root element.
    """

    elements_to_split = root.xpath(f".//*[@{separator_attr}]",namespaces=namespaces,)

    for elem in elements_to_split:
        if len(elem):
            raise ValueError(f"Element {elem.tag!r} contains child elements; only text-only elements can be split.")

        parent = elem.getparent()
        if parent is None:
            raise ValueError(
                "The root element has mf:separator and cannot be replaced "
                "with multiple sibling elements in-place.")

        clark_separator_attr = qname_to_clark(separator_attr, namespaces)
        separator = elem.attrib[clark_separator_attr]
        text = elem.text or ""

        parts = re.split(rf"\s*{re.escape(separator)}\s*", text)

        insert_at = parent.index(elem)

        # Copy attributes except mf:separator
        new_attrib = dict(elem.attrib)
        del new_attrib[clark_separator_attr]

        new_elements: list[ElementLike] = []
        for part in parts:
            new_elem = etree.Element(elem.tag, attrib=new_attrib, nsmap=elem.nsmap)
            new_elem.text = part
            new_elements.append(new_elem)

        # Preserve tail on the last element
        if new_elements:
            new_elements[-1].tail = elem.tail

        parent.remove(elem)

        for offset, new_elem in enumerate(new_elements):
            parent.insert(insert_at + offset, new_elem)