from __future__ import annotations

from urllib.parse import urlsplit


def is_absolute_http_url(value: str) -> bool:
    """
    Return True if `value` is an absolute HTTP(S) URL.

    Examples:
        >>> is_absolute_http_url("https://example.com")
        True
        >>> is_absolute_http_url("http://example.com/path?q=1")
        True
        >>> is_absolute_http_url("ftp://example.com")
        False
        >>> is_absolute_http_url("mailto:test@example.com")
        False
        >>> is_absolute_http_url("/relative/path")
        False
    """
    parts = urlsplit(value)
    return parts.scheme in {"http", "https"} and bool(parts.netloc)
