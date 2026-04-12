"""spark_feature_engine package."""

from .base import BaseSparkTransformer
from .discretisation import (
    ArbitraryDiscretiser,
    ArbitraryDiscretiserModel,
    EqualFrequencyDiscretiser,
    EqualFrequencyDiscretiserModel,
    EqualWidthDiscretiser,
    EqualWidthDiscretiserModel,
)
from .encoding import (
    CountFrequencyEncoder,
    CountFrequencyEncoderModel,
    OneHotEncoder,
    OneHotEncoderModel,
    OrdinalEncoder,
    OrdinalEncoderModel,
    RareLabelEncoder,
    RareLabelEncoderModel,
)
from .imputation import (
    ArbitraryNumberImputer,
    CategoricalImputer,
    DropMissingData,
    MeanMedianImputer,
    MeanMedianImputerModel,
)

__all__ = (
    "BaseSparkTransformer",
    "ArbitraryDiscretiser",
    "ArbitraryDiscretiserModel",
    "EqualFrequencyDiscretiser",
    "EqualFrequencyDiscretiserModel",
    "EqualWidthDiscretiser",
    "EqualWidthDiscretiserModel",
    "CountFrequencyEncoder",
    "CountFrequencyEncoderModel",
    "ArbitraryNumberImputer",
    "CategoricalImputer",
    "DropMissingData",
    "OneHotEncoder",
    "OneHotEncoderModel",
    "OrdinalEncoder",
    "OrdinalEncoderModel",
    "RareLabelEncoder",
    "RareLabelEncoderModel",
    "MeanMedianImputer",
    "MeanMedianImputerModel",
)

__version__ = "0.0.0"
