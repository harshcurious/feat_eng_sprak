"""Contract tests for the Spark-native relative feature creator."""

from __future__ import annotations

import math

import pytest

from spark_feature_engine.creation.relative_features import RelativeFeatures


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_relative_features_are_generated_for_selected_variables(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0, 4.0, "north"), (2, 8.0, 3.0, 2.0, "south")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, baseline DOUBLE, region STRING",
    )

    transformer = RelativeFeatures(
        variables=["income", "expenses"],
        reference=["baseline"],
        func=["sub", "add", "mul"],
    )
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert result[0].income_sub_baseline == pytest.approx(6.0)
    assert result[0].expenses_sub_baseline == pytest.approx(-2.0)
    assert result[0].income_add_baseline == pytest.approx(14.0)
    assert result[0].expenses_add_baseline == pytest.approx(6.0)
    assert result[0].income_mul_baseline == pytest.approx(40.0)
    assert result[0].expenses_mul_baseline == pytest.approx(8.0)
    assert result[1].income_sub_baseline == pytest.approx(6.0)
    assert result[1].expenses_sub_baseline == pytest.approx(1.0)


def test_relative_features_default_to_numeric_non_reference_variables(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0, 4.0, "north")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, baseline DOUBLE, region STRING",
    )

    transformer = RelativeFeatures(reference=["baseline"], func=["add"])
    transformed = transformer.transform(dataset)

    assert transformed.columns == [
        "row_id",
        "income",
        "expenses",
        "baseline",
        "region",
        "row_id_add_baseline",
        "income_add_baseline",
        "expenses_add_baseline",
    ]


def test_fill_value_is_used_for_zero_denominators(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 0.0), (2, 8.0, 2.0)],
        schema="row_id INT, income DOUBLE, baseline DOUBLE",
    )

    transformer = RelativeFeatures(
        variables=["income"],
        reference=["baseline"],
        func=["div", "mod", "floordiv"],
        fill_value=-1.0,
    )
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert result[0].income_div_baseline == pytest.approx(-1.0)
    assert result[0].income_mod_baseline == pytest.approx(-1.0)
    assert result[0].income_floordiv_baseline == pytest.approx(-1.0)
    assert result[1].income_div_baseline == pytest.approx(4.0)
    assert result[1].income_mod_baseline == pytest.approx(0.0)
    assert result[1].income_floordiv_baseline == pytest.approx(4.0)


def test_zero_denominators_raise_when_fill_value_is_not_configured(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 0.0)],
        schema="row_id INT, income DOUBLE, baseline DOUBLE",
    )

    transformer = RelativeFeatures(
        variables=["income"],
        reference=["baseline"],
        func=["div"],
    )

    with pytest.raises(ValueError, match="Division by zero"):
        transformer.transform(dataset)


def test_drop_original_removes_selected_and_reference_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0, 4.0, "north")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, baseline DOUBLE, region STRING",
    )

    transformer = RelativeFeatures(
        variables=["income", "expenses"],
        reference=["baseline"],
        func=["add"],
        drop_original=True,
    )
    transformed = transformer.transform(dataset)

    assert transformed.columns == [
        "row_id",
        "region",
        "income_add_baseline",
        "expenses_add_baseline",
    ]


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 2.0, 4.0, 99.0, "north"), (2, 8.0, 3.0, 2.0, 88.0, "south")],
        schema="row_id INT, income DOUBLE, expenses DOUBLE, baseline DOUBLE, untouched DOUBLE, region STRING",
    )

    transformer = RelativeFeatures(
        variables=["income"], reference=["baseline"], func=["pow"]
    )
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [99.0, 88.0]
    assert [row.region for row in result] == ["north", "south"]
    assert math.isclose(result[0].income_pow_baseline, 10000.0)
    assert math.isclose(result[1].income_pow_baseline, 64.0)


@pytest.mark.parametrize(
    ("variables", "reference", "func", "fill_value", "drop_original", "message"),
    [
        (["missing"], ["baseline"], ["add"], None, False, "missing"),
        (["income", "income"], ["baseline"], ["add"], None, False, "Duplicate"),
        (["income"], ["region"], ["add"], None, False, "numeric"),
        (["income"], ["baseline"], ["median"], None, False, "median"),
        (["income"], ["baseline"], ["div"], "invalid", False, "fill_value"),
        (["income"], ["baseline"], ["add"], None, "yes", "drop_original"),
    ],
)
def test_invalid_relative_feature_configuration_is_rejected(
    spark_session,
    variables,
    reference,
    func,
    fill_value,
    drop_original,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 4.0, "north")],
        schema="row_id INT, income DOUBLE, baseline DOUBLE, region STRING",
    )

    with pytest.raises((TypeError, ValueError), match=message):
        RelativeFeatures(
            variables=variables,
            reference=reference,
            func=func,
            fill_value=fill_value,
            drop_original=drop_original,
        ).transform(dataset)


def test_relative_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 4.0), (2, 8.0, 2.0)],
        schema="row_id INT, income DOUBLE, baseline DOUBLE",
    )

    transformer = RelativeFeatures(
        variables=["income"], reference=["baseline"], func=["add", "div"]
    )
    transformed = transformer.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "/" in plan_text or "div" in plan_text.lower()
