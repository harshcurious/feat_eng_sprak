"""Contract tests for recursive feature elimination."""

from __future__ import annotations

import pytest
from pyspark.ml.classification import RandomForestClassifier

from spark_feature_engine.selection.recursive_feature_elimination import (
    RecursiveFeatureElimination,
    RecursiveFeatureEliminationModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_recursive_feature_elimination_removes_dispensable_features(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 0.0, 1.0, 10.0),
            (2, 0, 2.0, 0.0, 1.0, 20.0),
            (3, 0, 3.0, 0.0, 1.0, 30.0),
            (4, 1, 2.0, 1.0, 1.0, 40.0),
            (5, 1, 9.0, 1.0, 1.0, 50.0),
            (6, 1, 10.0, 1.0, 1.0, 60.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, helper_num DOUBLE, stable_num DOUBLE, untouched DOUBLE",
    )

    model = RecursiveFeatureElimination(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "helper_num", "stable_num"],
        scoring="accuracy",
        threshold=0.0,
    ).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, RecursiveFeatureEliminationModel)
    assert "stable_num" in model.features_to_drop_
    assert len(model.features_to_drop_) >= 1
    assert set(model.feature_importances_) == {
        "shifted_num",
        "helper_num",
        "stable_num",
    }
    assert set(model.performance_drifts_) == {"shifted_num", "helper_num", "stable_num"}
    assert result.columns[0:2] == ["row_id", "target"]
    assert result.columns[-1] == "untouched"
    assert "stable_num" not in result.columns


def test_recursive_feature_elimination_rejects_incompatible_estimators(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(TypeError, match="Estimator"):
        RecursiveFeatureElimination(
            estimator=object(),
            target="target",
            variables=["shifted_num"],
            scoring="accuracy",
            threshold=0.0,
        ).fit(dataset)


def test_recursive_feature_elimination_is_deterministic_for_repeated_fits(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 0.0, 1.0),
            (2, 0, 2.0, 0.0, 1.0),
            (3, 0, 3.0, 0.0, 1.0),
            (4, 1, 2.0, 1.0, 1.0),
            (5, 1, 9.0, 1.0, 1.0),
            (6, 1, 10.0, 1.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, helper_num DOUBLE, stable_num DOUBLE",
    )

    selector = RecursiveFeatureElimination(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "helper_num", "stable_num"],
        scoring="accuracy",
        threshold=0.0,
    )

    first = selector.fit(dataset)
    second = selector.fit(dataset)

    assert first.feature_importances_ == second.feature_importances_
    assert first.performance_drifts_ == second.performance_drifts_
    assert first.features_to_drop_ == second.features_to_drop_


def test_recursive_feature_elimination_uses_native_spark_execution(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 0.0, 1.0),
            (2, 0, 2.0, 0.0, 1.0),
            (3, 1, 8.0, 1.0, 1.0),
            (4, 1, 9.0, 1.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, helper_num DOUBLE, stable_num DOUBLE",
    )

    model = RecursiveFeatureElimination(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "helper_num", "stable_num"],
        scoring="accuracy",
        threshold=0.0,
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
