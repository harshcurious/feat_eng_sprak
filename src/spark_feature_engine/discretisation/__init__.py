"""Discretisation package scaffolding for Phase 2."""

from __future__ import annotations

from .equal_frequency import EqualFrequencyDiscretiser, EqualFrequencyDiscretiserModel
from .equal_width import EqualWidthDiscretiser, EqualWidthDiscretiserModel

__all__ = (
    "EqualWidthDiscretiser",
    "EqualWidthDiscretiserModel",
    "EqualFrequencyDiscretiser",
    "EqualFrequencyDiscretiserModel",
)
