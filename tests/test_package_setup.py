"""Lightweight package and fixture wiring checks for Phase 1."""

from __future__ import annotations

import spark_feature_engine
from spark_feature_engine import (
    ArbitraryNumberImputer,
    BaseSparkTransformer,
    CategoricalImputer,
    DropMissingData,
    MeanMedianImputer,
    MeanMedianImputerModel,
)


def test_public_package_exports_are_available() -> None:
    expected = {
        "BaseSparkTransformer",
        "ArbitraryNumberImputer",
        "CategoricalImputer",
        "DropMissingData",
        "MeanMedianImputer",
        "MeanMedianImputerModel",
    }

    assert set(spark_feature_engine.__all__) == expected
    assert spark_feature_engine.__version__ == "0.0.0"
    assert BaseSparkTransformer is spark_feature_engine.BaseSparkTransformer
    assert ArbitraryNumberImputer is spark_feature_engine.ArbitraryNumberImputer
    assert CategoricalImputer is spark_feature_engine.CategoricalImputer
    assert DropMissingData is spark_feature_engine.DropMissingData
    assert MeanMedianImputer is spark_feature_engine.MeanMedianImputer
    assert MeanMedianImputerModel is spark_feature_engine.MeanMedianImputerModel


def test_shared_spark_fixture_is_local_and_deterministic(spark_session) -> None:
    assert spark_session.sparkContext.master == "local[1]"
    assert spark_session.sparkContext.appName == "spark-feature-engine-pytest"
    assert spark_session.conf.get("spark.sql.shuffle.partitions") == "1"
    assert spark_session.conf.get("spark.default.parallelism") == "1"
    assert spark_session.conf.get("spark.sql.session.timeZone") == "UTC"
