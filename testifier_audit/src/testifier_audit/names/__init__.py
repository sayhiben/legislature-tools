from __future__ import annotations

from testifier_audit.names.canonicalize import canonicalize_name
from testifier_audit.names.nickname_map import load_nickname_map
from testifier_audit.names.normalization import normalize_name_record

__all__ = ["canonicalize_name", "load_nickname_map", "normalize_name_record"]
