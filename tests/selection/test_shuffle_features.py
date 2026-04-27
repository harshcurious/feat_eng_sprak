"""Contract tests for shuffle-based feature selection."""

from __future__ import annotations

import pytest
from pyspark.ml.classification import LogisticRegression

from spark_feature_engine.selection.shuffle_features import (
    SelectByShuffling,
    SelectByShufflingModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_shuffle_selection_keeps_features_with_large_performance_drift(
    spark_session,
) -> None:
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

    model = SelectByShuffling(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.1,
        cv=2,
        random_state=17,
    ).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, SelectByShufflingModel)
    assert model.features_to_drop_ == ["stable_num"]
    assert model.performance_drifts_["shifted_num"] > 0.1
    assert model.performance_drifts_["stable_num"] <= 0.1
    assert result.columns == ["row_id", "target", "shifted_num", "untouched"]


def test_shuffle_selection_is_deterministic_for_fixed_random_state(
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

    selector = SelectByShuffling(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.1,
        cv=2,
        random_state=17,
    )

    first = selector.fit(dataset)
    second = selector.fit(dataset)

    assert first.performance_drifts_ == second.performance_drifts_
    assert first.features_to_drop_ == second.features_to_drop_


def test_shuffle_selection_rejects_incompatible_estimators(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(TypeError, match="Estimator"):
        SelectByShuffling(
            estimator=object(),
            target="target",
            variables=["shifted_num"],
            scoring="accuracy",
            cv=2,
            random_state=17,
        ).fit(dataset)


def test_shuffle_selection_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0),
            (2, 0, 2.0, 1.0),
            (3, 1, 8.0, 1.0),
            (4, 1, 9.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE",
    )

    model = SelectByShuffling(
        estimator=LogisticRegression(maxIter=20, regParam=0.0),
        target="target",
        variables=["shifted_num", "stable_num"],
        scoring="accuracy",
        threshold=0.1,
        cv=2,
        random_state=17,
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
