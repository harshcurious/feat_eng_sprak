"""Contract tests for single-feature performance selection."""

from __future__ import annotations

import pytest
from pyspark.ml.classification import LogisticRegression

from spark_feature_engine.selection.single_feature_performance import (
    SelectBySingleFeaturePerformance,
    SelectBySingleFeaturePerformanceModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_single_feature_performance_keeps_predictive_features(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0, 10.0),
            (2, 0, 2.0, 1.0, 20.0),
            (3, 0, 3.0, 1.0, 30.0),
            (4, 1, 8.0, 1.0, 40.0),
            (5, 1, 9.0, 1.0, 50.0),
            (6, 1, 10.0, 1.0, 60.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE, untouched DOUBLE",
    )

    model = SelectBySingleFeaturePerformance(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.75,
        cv=2,
    ).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, SelectBySingleFeaturePerformanceModel)
    assert model.features_to_drop_ == ["stable_num"]
    assert model.feature_performance_["shifted_num"] >= 0.75
    assert model.feature_performance_["stable_num"] < 0.75
    assert result.columns == ["row_id", "target", "shifted_num", "untouched"]


def test_single_feature_performance_is_deterministic_for_repeated_fits(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0),
            (2, 0, 2.0, 1.0),
            (3, 0, 3.0, 1.0),
            (4, 1, 8.0, 1.0),
            (5, 1, 9.0, 1.0),
            (6, 1, 10.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE",
    )

    selector = SelectBySingleFeaturePerformance(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.75,
        cv=2,
    )

    first = selector.fit(dataset)
    second = selector.fit(dataset)

    assert first.feature_performance_ == second.feature_performance_
    assert first.features_to_drop_ == second.features_to_drop_


def test_single_feature_performance_rejects_non_binary_target(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0), (2, 3.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(ValueError, match="binary"):
        SelectBySingleFeaturePerformance(
            estimator=LogisticRegression(maxIter=20, regParam=0.0),
            target="target",
            variables=["shifted_num"],
            scoring="accuracy",
            cv=2,
        ).fit(dataset)


def test_single_feature_performance_rejects_incompatible_estimators(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(TypeError, match="Estimator"):
        SelectBySingleFeaturePerformance(
            estimator=object(),
            target="target",
            variables=["shifted_num"],
            scoring="accuracy",
            cv=2,
        ).fit(dataset)


def test_single_feature_performance_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0),
            (2, 0, 2.0, 1.0),
            (3, 1, 8.0, 1.0),
            (4, 1, 9.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE",
    )

    model = SelectBySingleFeaturePerformance(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.75,
        cv=2,
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
