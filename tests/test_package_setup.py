"""Lightweight package and fixture wiring checks for Phase 1."""

from __future__ import annotations

import spark_feature_engine
from spark_feature_engine import (
    ArbitraryDiscretiser,
    ArbitraryDiscretiserModel,
    ArbitraryNumberImputer,
    BaseSparkTransformer,
    CountFrequencyEncoder,
    CountFrequencyEncoderModel,
    CyclicalFeatures,
    CyclicalFeaturesModel,
    CategoricalImputer,
    DropFeatures,
    DropConstantFeatures,
    DropConstantFeaturesModel,
    DropHighPSIFeatures,
    DropHighPSIFeaturesModel,
    DropCorrelatedFeatures,
    DropCorrelatedFeaturesModel,
    DropDuplicateFeatures,
    DropDuplicateFeaturesModel,
    DropMissingData,
    EqualFrequencyDiscretiser,
    EqualFrequencyDiscretiserModel,
    EqualWidthDiscretiser,
    EqualWidthDiscretiserModel,
    MeanMedianImputer,
    MeanMedianImputerModel,
    LogTransformer,
    MathFeatures,
    OneHotEncoder,
    OneHotEncoderModel,
    OutlierTrimmer,
    OutlierTrimmerModel,
    OrdinalEncoder,
    OrdinalEncoderModel,
    ProbeFeatureSelection,
    ProbeFeatureSelectionModel,
    PowerTransformer,
    RecursiveFeatureAddition,
    RecursiveFeatureAdditionModel,
    RecursiveFeatureElimination,
    RecursiveFeatureEliminationModel,
    RareLabelEncoder,
    RareLabelEncoderModel,
    RelativeFeatures,
    SelectByInformationValue,
    SelectByInformationValueModel,
    SelectBySingleFeaturePerformance,
    SelectBySingleFeaturePerformanceModel,
    SelectByShuffling,
    SelectByShufflingModel,
    SelectByTargetMeanPerformance,
    SelectByTargetMeanPerformanceModel,
    SmartCorrelatedSelection,
    SmartCorrelatedSelectionModel,
    Winsorizer,
    WinsorizerModel,
)


def test_public_package_exports_are_available() -> None:
    expected = {
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
        "DropFeatures",
        "DropConstantFeatures",
        "DropConstantFeaturesModel",
        "DropHighPSIFeatures",
        "DropHighPSIFeaturesModel",
        "DropCorrelatedFeatures",
        "DropCorrelatedFeaturesModel",
        "DropDuplicateFeatures",
        "DropDuplicateFeaturesModel",
        "ProbeFeatureSelection",
        "ProbeFeatureSelectionModel",
        "RecursiveFeatureAddition",
        "RecursiveFeatureAdditionModel",
        "RecursiveFeatureElimination",
        "RecursiveFeatureEliminationModel",
        "SelectByInformationValue",
        "SelectByInformationValueModel",
        "SelectBySingleFeaturePerformance",
        "SelectBySingleFeaturePerformanceModel",
        "SelectByShuffling",
        "SelectByShufflingModel",
        "SelectByTargetMeanPerformance",
        "SelectByTargetMeanPerformanceModel",
        "SmartCorrelatedSelection",
        "SmartCorrelatedSelectionModel",
        "LogTransformer",
        "PowerTransformer",
    }

    assert set(spark_feature_engine.__all__) == expected
    assert spark_feature_engine.__version__ == "0.0.0"
    assert BaseSparkTransformer is spark_feature_engine.BaseSparkTransformer
    assert CyclicalFeatures is spark_feature_engine.CyclicalFeatures
    assert CyclicalFeaturesModel is spark_feature_engine.CyclicalFeaturesModel
    assert MathFeatures is spark_feature_engine.MathFeatures
    assert RelativeFeatures is spark_feature_engine.RelativeFeatures
    assert DropConstantFeatures is spark_feature_engine.DropConstantFeatures
    assert DropConstantFeaturesModel is spark_feature_engine.DropConstantFeaturesModel
    assert DropCorrelatedFeatures is spark_feature_engine.DropCorrelatedFeatures
    assert (
        DropCorrelatedFeaturesModel is spark_feature_engine.DropCorrelatedFeaturesModel
    )
    assert DropDuplicateFeatures is spark_feature_engine.DropDuplicateFeatures
    assert DropDuplicateFeaturesModel is spark_feature_engine.DropDuplicateFeaturesModel
    assert SmartCorrelatedSelection is spark_feature_engine.SmartCorrelatedSelection
    assert (
        SmartCorrelatedSelectionModel
        is spark_feature_engine.SmartCorrelatedSelectionModel
    )
    assert ArbitraryDiscretiser is spark_feature_engine.ArbitraryDiscretiser
    assert ArbitraryDiscretiserModel is spark_feature_engine.ArbitraryDiscretiserModel
    assert EqualFrequencyDiscretiser is spark_feature_engine.EqualFrequencyDiscretiser
    assert (
        EqualFrequencyDiscretiserModel
        is spark_feature_engine.EqualFrequencyDiscretiserModel
    )
    assert EqualWidthDiscretiser is spark_feature_engine.EqualWidthDiscretiser
    assert EqualWidthDiscretiserModel is spark_feature_engine.EqualWidthDiscretiserModel
    assert CountFrequencyEncoder is spark_feature_engine.CountFrequencyEncoder
    assert CountFrequencyEncoderModel is spark_feature_engine.CountFrequencyEncoderModel
    assert ArbitraryNumberImputer is spark_feature_engine.ArbitraryNumberImputer
    assert CategoricalImputer is spark_feature_engine.CategoricalImputer
    assert DropMissingData is spark_feature_engine.DropMissingData
    assert OneHotEncoder is spark_feature_engine.OneHotEncoder
    assert OneHotEncoderModel is spark_feature_engine.OneHotEncoderModel
    assert OrdinalEncoder is spark_feature_engine.OrdinalEncoder
    assert OrdinalEncoderModel is spark_feature_engine.OrdinalEncoderModel
    assert RareLabelEncoder is spark_feature_engine.RareLabelEncoder
    assert RareLabelEncoderModel is spark_feature_engine.RareLabelEncoderModel
    assert MeanMedianImputer is spark_feature_engine.MeanMedianImputer
    assert MeanMedianImputerModel is spark_feature_engine.MeanMedianImputerModel
    assert OutlierTrimmer is spark_feature_engine.OutlierTrimmer
    assert OutlierTrimmerModel is spark_feature_engine.OutlierTrimmerModel
    assert Winsorizer is spark_feature_engine.Winsorizer
    assert WinsorizerModel is spark_feature_engine.WinsorizerModel
    assert DropFeatures is spark_feature_engine.DropFeatures
    assert LogTransformer is spark_feature_engine.LogTransformer
    assert PowerTransformer is spark_feature_engine.PowerTransformer
    assert DropHighPSIFeatures is spark_feature_engine.DropHighPSIFeatures
    assert DropHighPSIFeaturesModel is spark_feature_engine.DropHighPSIFeaturesModel
    assert ProbeFeatureSelection is spark_feature_engine.ProbeFeatureSelection
    assert ProbeFeatureSelectionModel is spark_feature_engine.ProbeFeatureSelectionModel
    assert RecursiveFeatureAddition is spark_feature_engine.RecursiveFeatureAddition
    assert (
        RecursiveFeatureAdditionModel
        is spark_feature_engine.RecursiveFeatureAdditionModel
    )
    assert (
        RecursiveFeatureElimination is spark_feature_engine.RecursiveFeatureElimination
    )
    assert (
        RecursiveFeatureEliminationModel
        is spark_feature_engine.RecursiveFeatureEliminationModel
    )
    assert SelectByInformationValue is spark_feature_engine.SelectByInformationValue
    assert (
        SelectByInformationValueModel
        is spark_feature_engine.SelectByInformationValueModel
    )
    assert (
        SelectBySingleFeaturePerformance
        is spark_feature_engine.SelectBySingleFeaturePerformance
    )
    assert (
        SelectBySingleFeaturePerformanceModel
        is spark_feature_engine.SelectBySingleFeaturePerformanceModel
    )
    assert SelectByShuffling is spark_feature_engine.SelectByShuffling
    assert SelectByShufflingModel is spark_feature_engine.SelectByShufflingModel
    assert (
        SelectByTargetMeanPerformance
        is spark_feature_engine.SelectByTargetMeanPerformance
    )
    assert (
        SelectByTargetMeanPerformanceModel
        is spark_feature_engine.SelectByTargetMeanPerformanceModel
    )


def test_shared_spark_fixture_is_local_and_deterministic(spark_session) -> None:
    assert spark_session.sparkContext.master == "local[1]"
    assert spark_session.sparkContext.appName == "spark-feature-engine-pytest"
    assert spark_session.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark_session.conf.get("spark.default.parallelism") == "1"
    assert spark_session.conf.get("spark.sql.session.timeZone") == "UTC"
