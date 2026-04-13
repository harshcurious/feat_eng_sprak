"""spark_feature_engine package."""

from .base import BaseSparkTransformer
from .creation import (
    CyclicalFeatures,
    CyclicalFeaturesModel,
    MathFeatures,
    RelativeFeatures,
)
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
from .outliers import OutlierTrimmer, OutlierTrimmerModel, Winsorizer, WinsorizerModel
from .transformation import LogTransformer, PowerTransformer

__all__ = (
    "BaseSparkTransformer",
    "CyclicalFeatures",
    "CyclicalFeaturesModel",
    "MathFeatures",
    "RelativeFeatures",
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
    "OutlierTrimmer",
    "OutlierTrimmerModel",
    "Winsorizer",
    "WinsorizerModel",
    "LogTransformer",
    "PowerTransformer",
)

__version__ = "0.0.0"
