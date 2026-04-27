"""Contract tests for target-mean performance feature selection."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.target_mean_selection import (
    SelectByTargetMeanPerformance,
    SelectByTargetMeanPerformanceModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_target_mean_performance_keeps_predictive_features(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 1.0, "A", 10.0),
            (2, 0, 2.0, 1.0, "A", 20.0),
            (3, 0, 3.0, 1.0, "A", 30.0),
            (4, 1, 8.0, 1.0, "B", 40.0),
            (5, 1, 9.0, 1.0, "B", 50.0),
            (6, 1, 10.0, 1.0, "B", 60.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, stable_num DOUBLE, shifted_cat STRING, untouched DOUBLE",
    )

    model = SelectByTargetMeanPerformance(
        target="target",
        variables=["shifted_num", "stable_num", "shifted_cat"],
        scoring="accuracy",
        threshold=0.75,
        bins=3,
        cv=2,
    ).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, SelectByTargetMeanPerformanceModel)
    assert set(model.features_to_drop_) == {"stable_num"}
    assert model.feature_performance_["shifted_num"] >= 0.75
    assert model.feature_performance_["shifted_cat"] >= 0.75
    assert model.feature_performance_["stable_num"] < 0.75
    assert result.columns == [
        "row_id",
        "target",
        "shifted_num",
        "shifted_cat",
        "untouched",
    ]


def test_target_mean_performance_supports_equal_frequency_binning(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0),
            (2, 0, 2.0),
            (3, 0, 3.0),
            (4, 1, 8.0),
            (5, 1, 9.0),
            (6, 1, 10.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE",
    )

    model = SelectByTargetMeanPerformance(
        target="target",
        variables=["shifted_num"],
        scoring="accuracy",
        threshold=0.75,
        bins=3,
        strategy="equal_frequency",
        cv=2,
    ).fit(dataset)

    assert model.features_to_drop_ == []
    assert model.feature_performance_["shifted_num"] >= 0.75


def test_target_mean_performance_preserves_non_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, 10.0),
            (2, 0, 2.0, 20.0),
            (3, 1, 8.0, 30.0),
            (4, 1, 9.0, 40.0),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, untouched DOUBLE",
    )

    model = SelectByTargetMeanPerformance(
        target="target",
        variables=["shifted_num"],
        scoring="accuracy",
        threshold=0.75,
        bins=2,
        cv=2,
    ).fit(dataset)
    rows = model.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in rows] == [10.0, 20.0, 30.0, 40.0]


def test_target_mean_performance_rejects_non_binary_target(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0), (2, 3.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(ValueError, match="binary"):
        SelectByTargetMeanPerformance(
            target="target",
            variables=["shifted_num"],
            scoring="accuracy",
            cv=2,
        ).fit(dataset)


@pytest.mark.parametrize(
    ("scoring", "threshold", "bins", "strategy", "cv", "message"),
    [
        ("f1", 0.75, 3, "equal_width", 2, "scoring"),
        ("accuracy", -0.1, 3, "equal_width", 2, "threshold"),
        ("accuracy", 0.75, 1, "equal_width", 2, "bins"),
        ("accuracy", 0.75, 3, "mystery", 2, "strategy"),
        ("accuracy", 0.75, 3, "equal_width", 1, "cv"),
    ],
)
def test_target_mean_performance_rejects_invalid_configuration(
    spark_session,
    scoring,
    threshold,
    bins,
    strategy,
    cv,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        SelectByTargetMeanPerformance(
            target="target",
            variables=["shifted_num"],
            scoring=scoring,
            threshold=threshold,
            bins=bins,
            strategy=strategy,
            cv=cv,
        ).fit(dataset)


def test_target_mean_performance_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0, 1.0, "A"),
            (2, 0, 2.0, "A"),
            (3, 1, 8.0, "B"),
            (4, 1, 9.0, "B"),
        ],
        schema="row_id INT, target INT, shifted_num DOUBLE, shifted_cat STRING",
    )

    model = SelectByTargetMeanPerformance(
        target="target",
        variables=["shifted_num", "shifted_cat"],
        scoring="accuracy",
        threshold=0.75,
        bins=2,
        cv=2,
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
