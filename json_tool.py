from __future__ import annotations

import json
from _typeshed import SupportsWrite
from collections.abc import Iterable, Mapping
from typing import Any, Optional


def write_json_array_from_iterable(
    items: Iterable[Mapping[str, Any]],
    fp: SupportsWrite[str],
    *,
    indent: Optional[int] = None,
    ensure_ascii: bool = False,
) -> None:
    """
    Streamově zapíše iterable slovníků jako JSON pole do otevřeného souboru.

    Funkce zapisuje objekty postupně (JSON-lines style),
    takže nedrží celé pole v paměti.

    Parameters
    ----------
    items:
        Iterable poskytující slovníky serializovatelné do JSON.
    fp:
        Otevřený textový soubor pro zápis.
    indent:
        Volitelné odsazení pro pretty JSON.
    ensure_ascii:
        Chování json.dumps pro Unicode.

    Example
    -------
    >>> data = ({"i": i} for i in range(3))
    >>> with open("out.json", "w", encoding="utf-8") as f:
    ...     write_json_array_from_iterable(data, f)
    """
    fp.write("[")
    first = True

    for item in items:
        if not first:
            fp.write(",")
        else:
            first = False

        json.dump(item, fp, indent=indent,ensure_ascii=ensure_ascii)

    fp.write("]")
