"""Discretisation package scaffolding for Phase 2."""

from __future__ import annotations

from .arbitrary import ArbitraryDiscretiser, ArbitraryDiscretiserModel
from .equal_frequency import EqualFrequencyDiscretiser, EqualFrequencyDiscretiserModel
from .equal_width import EqualWidthDiscretiser, EqualWidthDiscretiserModel

__all__ = (
    "ArbitraryDiscretiser",
    "ArbitraryDiscretiserModel",
    "EqualWidthDiscretiser",
    "EqualWidthDiscretiserModel",
    "EqualFrequencyDiscretiser",
    "EqualFrequencyDiscretiserModel",
)
