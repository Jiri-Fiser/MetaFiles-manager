import re

def glob_to_regex(pattern: str) -> str:
    """
    Convert a simplified path-glob to a regex.

    Rules:
      - The only '**' construct allowed is exactly '/**/' and matches any (possibly empty)
        path between slashes.
      - Outside of that construct, '*' and '?' do not match '/'.
      - Character classes '[...]' are copied unchanged into the regex.
      - The result is anchored with '^' and '$'.

    Examples:
    >>> re.fullmatch(glob_to_regex("a/b/c.txt"), "a/b/c.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/b/c.txt"), "a/b/c.tx") is not None
    False
    >>> re.fullmatch(glob_to_regex("a/*.txt"), "a/file.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/*.txt"), "a/x/y.txt") is not None
    False
    >>> re.fullmatch(glob_to_regex("a/?.txt"), "a/x.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/?.txt"), "a/xy.txt") is not None
    False
    >>> re.fullmatch(glob_to_regex("a/**/b.txt"), "a/b.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/**/b.txt"), "a/x/b.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/**/b.txt"), "a/x/y/z/b.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/**/b.txt"), "a/xb.txt") is not None
    False
    >>> re.fullmatch(glob_to_regex("a/[abc].txt"), "a/a.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/[abc].txt"), "a/d.txt") is not None
    False
    >>> re.fullmatch(glob_to_regex("a/[]].txt"), "a/].txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/[^]].txt"), "a/x.txt") is not None
    True
    >>> re.fullmatch(glob_to_regex("a/[^]].txt"), "a/].txt") is not None
    False
    >>> glob_to_regex("a/**b/c")
    Traceback (most recent call last):
    ...
    ValueError: Invalid pattern: '**' is only allowed as the exact substring '/**/'.
    >>> glob_to_regex("a/***/b")
    Traceback (most recent call last):
    ...
    ValueError: Invalid pattern: '**' is only allowed as the exact substring '/**/'.
    """
    if "**" in pattern.replace("/**/", ""):
        raise ValueError(
            "Invalid pattern: '**' is only allowed as the exact substring '/**/'."
        )

    _TOKEN_RE = re.compile(
        r"""
        (?P<deep>/\*\*/)|                          # exactly '/**/'
        (?P<class>\[[!^]?.[^]]*])|                 # character class '[...]' (non-empty)
        (?P<star>\*)|                              # '*'
        (?P<qmark>\?)|                             # '?'
        (?P<literal>([^[*?/]|/(?!\*\*/))+)         # literal character (negative lookahed for "/" is required)
        """,
        re.VERBOSE,
    )

    def repl(m: re.Match) -> str:
        g = m.groupdict()

        if g["deep"]:
            return r"/(?:[^/]+/)*"
        if g["class"]:
            cls = g["class"]
            if cls.startswith("[!"):
                cls = "[^" + cls[2:]
            return cls
        if g["star"]:
            return r"[^/]*"
        if g["qmark"]:
            return r"[^/]"
        if g["literal"]:
            return re.escape(g["literal"])

        raise ValueError(f"invalid pattern {pattern!r}")

    body = _TOKEN_RE.sub(repl, pattern)
    return f"^{body}$"


def canonicalize_path_pattern(path: str) -> str:
    """
    Canonicalize a file path pattern.

    Rules:
      - The pattern must be non-empty.
      - The root pattern '/' is forbidden.
      - Path segments '.' and '..' are forbidden.
      - Ensure the pattern starts with '/'.
      - Collapse consecutive slashes ('//' -> '/').
      - Trailing '/' is forbidden.

    Raises:
      ValueError if the pattern is empty, is '/', ends with '/',
      or contains '.'/'..' segments.

    Examples:
        >>> canonicalize_path_pattern("a/b")
        '/a/b'
        >>> canonicalize_path_pattern("/a//b///c")
        '/a/b/c'
        >>> canonicalize_path_pattern("a.txt/")
        Traceback (most recent call last):
        ...
        ValueError: invalid path pattern: trailing '/' is not allowed
        >>> canonicalize_path_pattern("/")
        Traceback (most recent call last):
        ...
        ValueError: invalid path pattern: '/' is not allowed
        >>> canonicalize_path_pattern("")
        Traceback (most recent call last):
        ...
        ValueError: invalid path pattern: empty string
        >>> canonicalize_path_pattern("a/../b")
        Traceback (most recent call last):
        ...
        ValueError: invalid path pattern: '.' or '..' segments are not allowed
    """
    if not path:
        raise ValueError("invalid path pattern: empty string")

    s = path if path.startswith("/") else "/" + path
    _SLASH_RUN_RE = re.compile(r"/+")
    s = _SLASH_RUN_RE.sub("/", s)

    if s == "/":
        raise ValueError("invalid path pattern: '/' is not allowed")

    if s.endswith("/"):
        raise ValueError("invalid path pattern: trailing '/' is not allowed")

    if any(seg in (".", "..") for seg in s.split("/")):
        raise ValueError("invalid path pattern: '.' or '..' segments are not allowed")

    return s



