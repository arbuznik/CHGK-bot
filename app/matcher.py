from __future__ import annotations

import re
import unicodedata
from typing import Iterable

_SPLIT_RE = re.compile(r"[\n;,/]|\s+или\s+", re.IGNORECASE)
_BRACKET_RE = re.compile(r"\[[^\]]*\]|\([^\)]*\)")
_NON_ALNUM_RE = re.compile(r"[^\w\s]+", re.UNICODE)
_SPACES_RE = re.compile(r"\s+")



def normalize_text(value: str) -> str:
    # Normalize Unicode to make accented variants and composed forms comparable.
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = value.replace("ё", "е").replace("Ё", "Е")
    value = value.lower().strip()
    value = _BRACKET_RE.sub(" ", value)
    value = _NON_ALNUM_RE.sub(" ", value)
    value = _SPACES_RE.sub(" ", value)
    return value.strip()



def _expand_candidates(values: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for value in values:
        if not value:
            continue
        for part in _SPLIT_RE.split(value):
            norm = normalize_text(part)
            if norm:
                out.add(norm)
    return out



def is_correct_answer(user_text: str, answer: str, zachet: str) -> bool:
    user_norm = normalize_text(user_text)
    if not user_norm:
        return False
    candidates = _expand_candidates([answer, zachet])
    if not candidates:
        return False
    return user_norm in candidates
