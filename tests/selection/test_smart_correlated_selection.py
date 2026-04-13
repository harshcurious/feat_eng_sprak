"""Contract tests for the Spark-native smart correlated selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.smart_correlated_selection import (
    SmartCorrelatedSelection,
    SmartCorrelatedSelectionModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_variance_strategy_keeps_highest_variance_representative(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 10.0, 0.0),
            (2, 2.0, 20.0, 1.0),
            (3, 3.0, 30.0, 0.0),
            (4, 4.0, 40.0, 1.0),
        ],
        schema="row_id INT, x1 DOUBLE, x2 DOUBLE, x3 DOUBLE",
    )

    model = SmartCorrelatedSelection(
        variables=["x1", "x2", "x3"],
        threshold=0.8,
        selection_method="variance",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, SmartCorrelatedSelectionModel)
    assert model.selected_features_ == ["x2"]
    assert model.features_to_drop_ == ["x1"]
    assert model.correlated_feature_sets_ == [["x1", "x2"]]
    assert result[0].asDict() == {"row_id": 1, "x2": 10.0, "x3": 0.0}


def test_missing_values_strategy_keeps_column_with_fewer_nulls(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1.0, 1.0), (2.0, None), (3.0, 3.0), (4.0, None)],
        schema="x1 DOUBLE, x2 DOUBLE",
    )

    model = SmartCorrelatedSelection(
        variables=["x1", "x2"],
        threshold=0.8,
        selection_method="missing_values",
    ).fit(dataset)

    assert model.selected_features_ == ["x1"]
    assert model.features_to_drop_ == ["x2"]


@pytest.mark.parametrize(
    ("selection_method", "message"),
    [
        ("model_performance", "model_performance"),
        ("corr_with_target", "corr_with_target"),
    ],
)
def test_unsupported_smart_selection_methods_are_rejected(
    spark_session,
    selection_method,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1.0, 2.0), (2.0, 4.0)], schema="x1 DOUBLE, x2 DOUBLE"
    )

    with pytest.raises(ValueError, match=message):
        SmartCorrelatedSelection(
            variables=["x1", "x2"],
            selection_method=selection_method,
        ).fit(dataset)


def test_smart_correlated_selection_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)],
        schema="x1 DOUBLE, x2 DOUBLE",
    )

    model = SmartCorrelatedSelection(
        variables=["x1", "x2"],
        threshold=0.8,
        selection_method="variance",
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
