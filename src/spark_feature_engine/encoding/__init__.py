"""Encoding package scaffolding for Phase 2."""

from __future__ import annotations

from .count_frequency import CountFrequencyEncoder, CountFrequencyEncoderModel
from .one_hot import OneHotEncoder, OneHotEncoderModel
from .ordinal import OrdinalEncoder, OrdinalEncoderModel
from .rare_label import RareLabelEncoder, RareLabelEncoderModel

__all__ = (
    "CountFrequencyEncoder",
    "CountFrequencyEncoderModel",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "OrdinalEncoder",
    "OrdinalEncoderModel",
    "RareLabelEncoder",
    "RareLabelEncoderModel",
)
