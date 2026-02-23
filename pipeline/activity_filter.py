from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Set, Optional

import yaml


def _norm(s: str) -> str:
    return s.strip().lower().replace(" ", "_")


@dataclass(frozen=True)
class ActivityFilter:
    include_types: Set[str]
    exclude_types: Set[str]

    def allows(self, activity_type: Optional[str]) -> bool:
        if not activity_type:
            return False
        t = _norm(activity_type)
        if t in self.exclude_types:
            return False
        return t in self.include_types


def load_activity_filter() -> ActivityFilter:
    """
    Loads activity_filters.yaml from repo root by default.
    In Cloud Run, we can override with ACTIVITY_FILTER_PATH.
    """
    default_path = Path(__file__).resolve().parent / "activity_filters.yaml"
    cfg_path = Path(os.getenv("ACTIVITY_FILTER_PATH", str(default_path)))

    if not cfg_path.exists():
        raise FileNotFoundError(f"Activity filter file not found: {cfg_path}")

    data: Dict[str, Any] = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    include: Set[str] = set()
    include_groups = (data.get("include") or {})
    for _, types in include_groups.items():
        include |= {_norm(t) for t in (types or [])}

    exclude: Set[str] = {_norm(t) for t in (data.get("exclude") or [])}

    return ActivityFilter(include_types=include, exclude_types=exclude)
