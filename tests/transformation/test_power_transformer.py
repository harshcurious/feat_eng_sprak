"""Contract tests for the Spark-native power transformer."""

from __future__ import annotations

import pytest

from spark_feature_engine.transformation.power_transformer import PowerTransformer


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_default_exponent_transforms_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.0, 3.0, "keep"), (2, 4.0, 5.0, "stay")],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    transformer = PowerTransformer(variables=["x", "y"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.x for row in result] == pytest.approx([4.0, 16.0])
    assert [row.y for row in result] == pytest.approx([9.0, 25.0])


def test_explicit_exponent_transforms_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.0, 3.0), (2, 4.0, 5.0)], schema="row_id INT, x DOUBLE, y DOUBLE"
    )

    transformer = PowerTransformer(variables=["x", "y"], exponent=3)
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.x for row in result] == pytest.approx([8.0, 64.0])
    assert [row.y for row in result] == pytest.approx([27.0, 125.0])


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.0, 10.0, "alpha"), (2, 3.0, 20.0, "beta")],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    transformer = PowerTransformer(variables=["x"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [10.0, 20.0]
    assert [row.label for row in result] == ["alpha", "beta"]


@pytest.mark.parametrize(
    ("variables", "exponent", "message"),
    [
        (["missing"], 2, "missing"),
        (["x", "x"], 2, "Duplicate"),
        (["x"], "invalid", "exponent"),
    ],
)
def test_validation_failures_are_rejected(
    spark_session, variables, exponent, message
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.0, "alpha")], schema="row_id INT, x DOUBLE, label STRING"
    )

    transformer = PowerTransformer(variables=variables, exponent=exponent)

    with pytest.raises((ValueError, TypeError), match=message):
        transformer.transform(dataset)


def test_power_transformer_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.0), (2, 4.0)], schema="row_id INT, x DOUBLE"
    )

    transformer = PowerTransformer(variables=["x"], exponent=3)
    transformed = transformer.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "pow" in plan_text.lower()
