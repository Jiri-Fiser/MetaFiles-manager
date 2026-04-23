from typing import Mapping, Iterable, TypeVar, List, MutableMapping, Callable, Dict

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")

def merge_mapping_values(mapping: Mapping[object, Iterable[T]]) -> List[T]:
    """
    Sloučí všechny hodnoty mappingu (iterovatelné kolekce) do jednoho seznamu.

    >>> merge_mapping_values({"a": [1, 2], "b": [3], "c": []})
    [1, 2, 3]
    """
    result: List[T] = []
    for values in mapping.values():
        result.extend(values)
    return result


def merge_maplists(
    target: MutableMapping[K, List[V]],
    source: Mapping[K, List[V]],
) -> None:
    """
    Append items from `source` into `target`.

    For each key in `source`:
    - if the key does not exist in `target`, a new list is created
    - if the key exists, items are appended to the existing list

    The function mutates `target` in place.
    """
    for key, values in source.items():
        if key in target:
            target[key].extend(values)
        else:
            target[key] = list(values)


def groupby(items: Iterable[T], key: Callable[[T], K] = lambda x:x) -> Dict[K, List[T]]:
    """
    Group items by key(item).

    :param items: input iterable
    :param key: function computing a key for each item (default: identity)
    :return: dict mapping keys to lists of items
    """
    result: Dict[K, List[T]] = {}
    for item in items:
        k = key(item)
        if k in result:
            result[k].append(item)
        else:
            result[k] = [item]
    return result

