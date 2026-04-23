from collections import defaultdict
from pathlib import Path
from typing import List, Union, Mapping, MutableMapping, Set, MutableSequence

from sqlalchemy.ext.mutable import MutableComposite

from metafiles_parser import MFRule
from xml_tools import ElementLike
from datetime import datetime


def extend_defaultset(
    target: MutableMapping[str, Set[ElementLike]],
    source: Mapping,
) -> None:
    for k, v in source.items():
        target[k].update(v)

class FileMatcher:
    def __init__(self, rules: List[MFRule]):
        self.rules = rules

    def process_subtree(self, subtree: Union[Path, str]):
        subtree = Path(subtree)
        for path in subtree.rglob("*"):
            if path.is_file():
                if path.name in ["metafile.xml"]:
                    continue
                filename = "/" + str(path.relative_to(subtree))
                file_metadata: MutableMapping[str, Set[ElementLike]] = defaultdict(set)
                applied_rules: MutableSequence[str] = []
                for rule in self.rules:
                    if rule.re_pattern.match(filename):
                        applied_rules.append(rule.pattern)
                        extend_defaultset(file_metadata, rule.metadata)
                mtime = path.stat().st_mtime
                substitutions: Mapping[str, str] = {"localName": str(path.relative_to(subtree)),
                                 "size": str(path.stat().st_size),
                                 "mtime": datetime.fromtimestamp(mtime).strftime("%Y-%m-%dT%H:%MZ"),
                                 "mf_rules": ", ".join(applied_rules)}
                yield path, file_metadata, substitutions

