import re
from urllib.parse import quote_plus

path_splitter = re.compile(
    r"""(ark|ARK):/?(?P<naan>[0-9]+) # NAAN fixed prefix
    /+
    (?P<shoulder>[a-z-]+[0-9]+)?  #shoulder (name space)
    (?P<locid>.*) #id in shoulder namespace
    """, flags=re.X)


class ArkFormatError(Exception):
    pass


def insert_separator(s: str, n: int, sep: str) -> str:
    """Vloží oddělovač 'sep' mezi každých 'n' znaků v řetězci 's'."""
    return sep.join(s[i:i+n] for i in range(0, len(s), n))


class ArkIdentifier:
    def __init__(self, naan, shoulder, locid):
        self.naan = naan
        self.shoulder = shoulder
        self.locid = ArkIdentifier.normalize_id(locid)

    def __str__(self):
        """
        User readable form of ARK identifier (primary use: displayed or printed test)
        :return:
        """
        return f"ark:/{self.naan}/{self.shoulder}-{insert_separator(self.locid, 6, '-')}"

    def __repr__(self):
        """
        Canonical form of ARK identifier (primary use: part of URLs)
        :return:
        """
        return f"ark:/{self.naan}/{self.shoulder}{ArkIdentifier.normalize_id(self.locid)}"

    def url(self, resolver: str = "https://n2t.org") -> str:
        return f"{resolver}/{str(self)}"

    def __eq__(self, other):
        return self.naan == other.naan and self.shoulder == other.shoulder and self.locid == other.locid

    def __hash__(self):
        return hash((self.naan, self.shoulder, self.locid))

    @staticmethod
    def normalize_id(ident: str) -> str:
        def to_upper(match):
            return f"%{match.group(1).upper()}"
        ident = quote_plus(ident)
        ident = ident.replace("-", "")
        ident = ident.rstrip("/")
        ident = re.sub("([/.])[\1]+", r"\1", ident)
        ident = re.sub(r'%([0-9a-fA-F]{2})', to_upper, ident)
        return ident

    @staticmethod
    def create(naan: str, shoulder: str, locid: str) -> 'ArkIdentifier':
        return ArkIdentifier(naan, shoulder, ArkIdentifier.normalize_id(locid))

    @staticmethod
    def parse(ark_string: str) -> 'ArkIdentifier':
        match = path_splitter.match(ark_string)
        if not match:
            raise ArkFormatError(f"Identifier `{ark_string}` is not parsable ARK identifier")
        return ArkIdentifier.create(match.group("naan"), match.group("shoulder"),
                                    ArkIdentifier.normalize_id(match.group("locid")))