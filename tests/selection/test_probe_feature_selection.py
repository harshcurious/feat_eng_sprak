"""Contract tests for collective-mode probe feature selection."""

from __future__ import annotations

import pytest
from pyspark.ml.classification import RandomForestClassifier

from spark_feature_engine.selection.probe_feature_selection import (
    ProbeFeatureSelection,
    ProbeFeatureSelectionModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_probe_feature_selection_drops_features_weaker_than_probes(
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

    model = ProbeFeatureSelection(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "stable_num"],
        n_probes=2,
        distribution="normal",
        threshold="mean",
        random_state=17,
    ).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, ProbeFeatureSelectionModel)
    assert len(model.probe_features_) == 2
    assert model.features_to_drop_ == ["stable_num"]
    assert model.feature_importances_["shifted_num"] > model.probe_threshold_
    assert model.feature_importances_["stable_num"] <= model.probe_threshold_
    assert result.columns == ["row_id", "target", "shifted_num", "untouched"]


def test_probe_feature_selection_is_deterministic_for_fixed_random_state(
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

    selector = ProbeFeatureSelection(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "stable_num"],
        n_probes=2,
        distribution="normal",
        threshold="mean",
        random_state=17,
    )

    first = selector.fit(dataset)
    second = selector.fit(dataset)

    assert first.probe_features_ == second.probe_features_
    assert first.feature_importances_ == second.feature_importances_
    assert first.features_to_drop_ == second.features_to_drop_


@pytest.mark.parametrize(
    ("distribution", "threshold", "n_probes", "message"),
    [
        ("mystery", "mean", 2, "distribution"),
        ("normal", "median", 2, "threshold"),
        ("normal", "mean", 0, "n_probes"),
    ],
)
def test_probe_feature_selection_rejects_invalid_configuration(
    spark_session,
    distribution,
    threshold,
    n_probes,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        ProbeFeatureSelection(
            estimator=RandomForestClassifier(numTrees=5, maxDepth=2, seed=17),
            target="target",
            variables=["shifted_num"],
            n_probes=n_probes,
            distribution=distribution,
            threshold=threshold,
            random_state=17,
        ).fit(dataset)


def test_probe_feature_selection_rejects_incompatible_estimators(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(TypeError, match="Estimator"):
        ProbeFeatureSelection(
            estimator=object(),
            target="target",
            variables=["shifted_num"],
            n_probes=2,
            distribution="normal",
            threshold="mean",
            random_state=17,
        ).fit(dataset)


def test_probe_feature_selection_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0),
            (2, 0, 2.0, 1.0),
            (3, 1, 8.0, 1.0),
            (4, 1, 9.0, 1.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE",
    )

    model = ProbeFeatureSelection(
        estimator=RandomForestClassifier(numTrees=10, maxDepth=3, seed=17),
        target="target",
        variables=["shifted_num", "stable_num"],
        n_probes=2,
        distribution="normal",
        threshold="mean",
        random_state=17,
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
