import json
from enum import Enum
from pathlib import Path
from typing import Type, Dict, Sequence, Mapping, Tuple

from filehash import hash_filename, hash_context
from fntrans import bcode


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.name   # použije symbolické jméno
        return super().default(obj)

def make_multi_enum_decoder(enum_map: Mapping[str, Type[Enum]]):
    """
    enum_map: mapování 'json_klíč' -> Enum třída
    """
    # Předpočítáme case-insensitive look-up tabulky pro každý klíč
    lookups: Dict[str, Tuple[Type[Enum], Dict[str, Enum]]] = {}
    for key, enum_cls in enum_map.items():
        lut = {name.casefold(): member for name, member in enum_cls.__members__.items()}
        lookups[key] = (enum_cls, lut)

    def decode_data(dct: dict):
        for k, v in list(dct.items()):
            if k not in lookups:
                continue

            enum_cls, lut = lookups[k]

            def convert_one(s: str) -> Enum:
                member = lut.get(s.casefold())
                if member is None:
                    allowed = ", ".join(enum_cls.__members__.keys())
                    raise ValueError(
                        f"Invalid value '{s}' for enum {enum_cls.__name__} at key '{k}'. "
                        f"Allowed: {allowed}"
                    )
                return member

            if isinstance(v, str):
                dct[k] = convert_one(v)
            elif isinstance(v, Sequence):
                converted = []
                for idx, item in enumerate(v):
                    if not isinstance(item, str):
                        raise ValueError(
                            f"Expected string in list for enum {enum_cls.__name__} at key '{k}' "
                            f"on index {idx}, got {type(item).__name__}"
                        )
                    converted.append(convert_one(item))
                dct[k] = converted
            else:
                raise ValueError(
                    f"Expected string or list of strings for enum {enum_cls.__name__} at key '{k}', "
                    f"got {type(v).__name__}"
                )

        return dct
    return decode_data


class ConflictAction(Enum):
    IGNORE = "ignore"     # ponechat původní hodnotu (neupdateovat)
    WARNING = "warning"   # updatovat a zalogovat varování
    STRICT = "strict"     # vyhodit výjimku (celou operaci zrušit)
    UPDATE = "update"     # updatovat tiše (default pro nevyjmenované atributy)

class NameStrategy(Enum):
    FILENAME_HASH_12 = 0
    CONTEXT_HASH_12 = 1
    FILENAME_BCODE = 2
    FILE_NAME = 3

def parse_policy(json_text: str):
    decoder = make_multi_enum_decoder({
        "local_path": ConflictAction,
        "digest": ConflictAction,
        "meta": ConflictAction,
        "links": ConflictAction,
        "local_name_strategy": NameStrategy,
    })
    data = json.loads(json_text, object_hook=decoder)
    return data

def get_localname(path: Path, data_path: Path, strategy: NameStrategy) -> str:
    local_path = str(path.relative_to(data_path))
    if strategy == NameStrategy.FILENAME_HASH_12:
        return hash_filename(local_path, "shake_128", 12)
    elif strategy == NameStrategy.CONTEXT_HASH_12:
        return hash_context(path, "shake_128", 12)
    elif strategy == NameStrategy.FILENAME_BCODE:
        return bcode(local_path)
    elif strategy == NameStrategy.FILE_NAME:
        return path.name
    else:
        raise ValueError(f"Unknown strategy '{strategy}'")

if __name__ == "__main__":
    data = {"strict": ConflictAction.STRICT, "update": ConflictAction.UPDATE}
    json_str = json.dumps(data, cls=EnumEncoder, indent=2)
    print(json_str)

    decoder = make_multi_enum_decoder({
        "strict": ConflictAction,
        "update": ConflictAction,
    })

    data = json.loads(json_str, object_hook=decoder)
    print(data)


