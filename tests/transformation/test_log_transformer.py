"""Contract tests for the Spark-native log transformer."""

from __future__ import annotations

import math

import pytest

from spark_feature_engine.transformation.log_transformer import LogTransformer


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_positive_inputs_are_transformed_logarithmically(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 10.0, "keep"), (2, math.e, 100.0, "stay")],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    transformer = LogTransformer(variables=["x", "y"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.x for row in result] == pytest.approx([0.0, 1.0])
    assert [row.y for row in result] == pytest.approx([math.log(10.0), math.log(100.0)])


@pytest.mark.parametrize(
    "dataset_factory",
    [
        lambda spark_session: spark_session.createDataFrame(
            [(1, 0.0)], schema="row_id INT, x DOUBLE"
        ),
        lambda spark_session: spark_session.createDataFrame(
            [(1, -1.0)], schema="row_id INT, x DOUBLE"
        ),
    ],
)
def test_zero_and_negative_inputs_are_rejected(spark_session, dataset_factory) -> None:
    dataset = dataset_factory(spark_session)
    transformer = LogTransformer(variables=["x"])

    with pytest.raises((ValueError, TypeError), match="positive|zero|negative|log"):
        transformer.transform(dataset)


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 10.0, "alpha"), (2, math.e, 20.0, "beta")],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    transformer = LogTransformer(variables=["x"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [10.0, 20.0]
    assert [row.label for row in result] == ["alpha", "beta"]


@pytest.mark.parametrize(
    ("variables", "message"),
    [
        (["missing"], "missing"),
        (["x", "x"], "Duplicate"),
    ],
)
def test_validation_failures_are_rejected(spark_session, variables, message) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, "alpha")], schema="row_id INT, x DOUBLE, label STRING"
    )

    transformer = LogTransformer(variables=variables)

    with pytest.raises((ValueError, KeyError, TypeError), match=message):
        transformer.transform(dataset)


def test_log_transformer_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, math.e)], schema="row_id INT, x DOUBLE"
    )

    transformer = LogTransformer(variables=["x"])
    transformed = transformer.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "log" in plan_text.lower()
