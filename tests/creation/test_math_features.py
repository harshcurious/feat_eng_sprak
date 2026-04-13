"""Contract tests for the Spark-native math feature creator."""

from __future__ import annotations

import pytest

from spark_feature_engine.creation.math_features import MathFeatures


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_sum_feature_is_added_for_selected_variables(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 3.0, "north"), (2, 7.0, 2.0, "south")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, region STRING",
    )

    transformer = MathFeatures(variables=["income", "expenses"], func=["sum"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert result[0].asDict() == {
        "row_id": 1,
        "income": 10.0,
        "expenses": 3.0,
        "region": "north",
        "sum_income_expenses": pytest.approx(13.0),
    }
    assert result[1].asDict() == {
        "row_id": 2,
        "income": 7.0,
        "expenses": 2.0,
        "region": "south",
        "sum_income_expenses": pytest.approx(9.0),
    }


def test_multiple_functions_create_multiple_output_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0), (2, 4.0, 6.0)], schema="row_id INT, x DOUBLE, y DOUBLE"
    )

    transformer = MathFeatures(variables=["x", "y"], func=["sum", "mean", "prod"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert result[0].sum_x_y == pytest.approx(12.0)
    assert result[0].mean_x_y == pytest.approx(6.0)
    assert result[0].prod_x_y == pytest.approx(20.0)
    assert result[1].sum_x_y == pytest.approx(10.0)
    assert result[1].mean_x_y == pytest.approx(5.0)
    assert result[1].prod_x_y == pytest.approx(24.0)


def test_explicit_new_variable_names_are_used(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0), (2, 4.0, 6.0)], schema="row_id INT, x DOUBLE, y DOUBLE"
    )

    transformer = MathFeatures(
        variables=["x", "y"],
        func=["min", "max"],
        new_variable_names=["smallest_value", "largest_value"],
    )
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert result[0].smallest_value == pytest.approx(2.0)
    assert result[0].largest_value == pytest.approx(10.0)
    assert result[1].smallest_value == pytest.approx(4.0)
    assert result[1].largest_value == pytest.approx(6.0)


def test_drop_original_removes_selected_source_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 3.0, "north")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, region STRING",
    )

    transformer = MathFeatures(
        variables=["income", "expenses"],
        func=["sum"],
        drop_original=True,
    )
    transformed = transformer.transform(dataset)

    assert transformed.columns == ["row_id", "region", "sum_income_expenses"]


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 3.0, 99.0, "north"), (2, 7.0, 2.0, 88.0, "south")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, untouched DOUBLE, region STRING",
    )

    transformer = MathFeatures(variables=["income", "expenses"], func=["sum"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [99.0, 88.0]
    assert [row.region for row in result] == ["north", "south"]


@pytest.mark.parametrize(
    ("variables", "func", "new_variable_names", "message"),
    [
        (["missing", "expenses"], ["sum"], None, "missing"),
        (["income", "income"], ["sum"], None, "Duplicate"),
        (["income"], ["sum"], None, "at least 2"),
        (["income", "region"], ["sum"], None, "numeric"),
        (["income", "expenses"], ["median"], None, "median"),
        (
            ["income", "expenses"],
            ["sum"],
            ["total", "average"],
            "number of new feature names",
        ),
        (["income", "expenses"], ["sum"], ["region"], "collide"),
    ],
)
def test_invalid_math_feature_configuration_is_rejected(
    spark_session,
    variables,
    func,
    new_variable_names,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 3.0, "north")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, region STRING",
    )

    with pytest.raises((TypeError, ValueError), match=message):
        MathFeatures(
            variables=variables,
            func=func,
            new_variable_names=new_variable_names,
        ).transform(dataset)


def test_math_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 3.0), (2, 7.0, 2.0)],
        schema="row_id INT, income DOUBLE, expenses DOUBLE",
    )

    transformer = MathFeatures(variables=["income", "expenses"], func=["sum", "prod"])
    transformed = transformer.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "+" in plan_text or "add" in plan_text.lower()
