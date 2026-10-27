import hashlib
import re

MAX_IDENTIFIER_LEN = 63

_UNSAFE_CHARS = re.compile(r"[^a-z0-9_]")
_REPEATED_UNDERSCORES = re.compile(r"_+")


def sanitize_identifier(raw: object, fallback: str = "unnamed") -> str:
    """Turn an untrusted name (file name, header) into a lowercase [a-z_][a-z0-9_]* identifier."""
    name = _UNSAFE_CHARS.sub("_", str(raw).lower())
    name = _REPEATED_UNDERSCORES.sub("_", name).strip("_")
    if not name:
        name = fallback
    if not (name[0].isalpha() or name[0] == "_"):
        name = f"_{name}"
    return truncate_identifier(name)


def truncate_identifier(name: str) -> str:
    """Shorten to the Postgres identifier limit, keeping distinct names distinct via a hash suffix."""
    if len(name) <= MAX_IDENTIFIER_LEN:
        return name
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    return f"{name[:MAX_IDENTIFIER_LEN - len(digest) - 1]}_{digest}"


def quote_identifier(name: object) -> str:
    """Render a name as a single double-quoted SQL identifier, escaping embedded quotes."""
    text = str(name)
    if not text or "\x00" in text:
        raise ValueError(f"Invalid SQL identifier: {text!r}")
    return '"' + text.replace('"', '""') + '"'


def unique_identifier(name: str, used: set) -> str:
    """Return `name`, or `name_2`, `name_3`, ... if already in `used`; records the result in `used`."""
    candidate = name
    suffix = 2
    while candidate in used:
        candidate = truncate_identifier(f"{name}_{suffix}")
        suffix += 1
    used.add(candidate)
    return candidate
