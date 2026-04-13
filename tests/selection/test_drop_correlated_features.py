"""Contract tests for the Spark-native correlated feature selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.drop_correlated_features import (
    DropCorrelatedFeatures,
    DropCorrelatedFeaturesModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_correlated_features_are_learned_and_removed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 2.0, 0.0),
            (2, 2.0, 4.0, 1.0),
            (3, 3.0, 6.0, 0.0),
            (4, 4.0, 8.0, 1.0),
        ],
        schema="row_id INT, x1 DOUBLE, x2 DOUBLE, x3 DOUBLE",
    )

    model = DropCorrelatedFeatures(variables=["x1", "x2", "x3"], threshold=0.8).fit(
        dataset
    )
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, DropCorrelatedFeaturesModel)
    assert model.features_to_drop_ == ["x2"]
    assert model.correlated_feature_sets_ == [["x1", "x2"]]
    assert result[0].asDict() == {"row_id": 1, "x1": 1.0, "x3": 0.0}


def test_non_selected_columns_remain_unchanged_for_correlation_selector(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, "north"), (2, 2.0, 4.0, "south"), (3, 3.0, 6.0, "east")],
        schema="row_id INT, x1 DOUBLE, x2 DOUBLE, region STRING",
    )

    model = DropCorrelatedFeatures(variables=["x1", "x2"], threshold=0.8).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.region for row in result] == ["north", "south", "east"]
    assert [row.row_id for row in result] == [1, 2, 3]


@pytest.mark.parametrize(
    ("variables", "threshold", "method", "message"),
    [
        (["missing", "x2"], 0.8, "pearson", "missing"),
        (["x1", "label"], 0.8, "pearson", "numeric"),
        (["x1"], 0.8, "pearson", "at least 2"),
        (["x1", "x2"], 1.1, "pearson", "threshold"),
        (["x1", "x2"], 0.8, "kendall", "kendall"),
    ],
)
def test_invalid_correlated_selector_configuration_is_rejected(
    spark_session,
    variables,
    threshold,
    method,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, "north")],
        schema="row_id INT, x1 DOUBLE, x2 DOUBLE, label STRING",
    )

    with pytest.raises((TypeError, ValueError), match=message):
        DropCorrelatedFeatures(
            variables=variables,
            threshold=threshold,
            method=method,
        ).fit(dataset)


def test_drop_correlated_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0), (2, 2.0, 4.0), (3, 3.0, 6.0)],
        schema="row_id INT, x1 DOUBLE, x2 DOUBLE",
    )

    model = DropCorrelatedFeatures(variables=["x1", "x2"], threshold=0.8).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
