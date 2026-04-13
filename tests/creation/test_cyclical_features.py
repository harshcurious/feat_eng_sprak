"""Contract tests for the Spark-native cyclical feature creator."""

from __future__ import annotations

import math

import pytest

from spark_feature_engine.creation.cyclical_features import (
    CyclicalFeatures,
    CyclicalFeaturesModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_fit_learns_max_values_and_adds_sine_cosine_features(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, "north"), (2, 4.0, 8.0, "south")],
        schema="row_id INT, month DOUBLE, hour DOUBLE, region STRING",
    )

    model = CyclicalFeatures(variables=["month", "hour"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, CyclicalFeaturesModel)
    assert model.variables_ == ["month", "hour"]
    assert model.max_values_ == {"month": 4.0, "hour": 8.0}
    assert math.isclose(result[0].month_sin, 1.0, rel_tol=1e-9)
    assert math.isclose(result[0].month_cos, 0.0, abs_tol=1e-9)
    assert math.isclose(result[0].hour_sin, 1.0, rel_tol=1e-9)
    assert math.isclose(result[0].hour_cos, 0.0, abs_tol=1e-9)
    assert math.isclose(result[1].month_sin, 0.0, abs_tol=1e-9)
    assert math.isclose(result[1].month_cos, 1.0, abs_tol=1e-9)


def test_configured_max_values_are_preserved_on_fitted_model(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0), (2, 4.0, 8.0)], schema="row_id INT, month DOUBLE, hour DOUBLE"
    )

    model = CyclicalFeatures(
        variables=["month", "hour"], max_values={"month": 12.0, "hour": 24.0}
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.max_values_ == {"month": 12.0, "hour": 24.0}
    assert math.isclose(result[0].month_sin, 0.5, rel_tol=1e-9)
    assert math.isclose(result[0].month_cos, math.sqrt(3) / 2.0, rel_tol=1e-9)
    assert math.isclose(result[0].hour_sin, 0.5, rel_tol=1e-9)
    assert math.isclose(result[0].hour_cos, math.sqrt(3) / 2.0, rel_tol=1e-9)


def test_drop_original_removes_source_columns_after_transform(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, "north")],
        schema="row_id INT, month DOUBLE, hour DOUBLE, region STRING",
    )

    model = CyclicalFeatures(variables=["month", "hour"], drop_original=True).fit(
        dataset
    )
    transformed = model.transform(dataset)

    assert transformed.columns == [
        "row_id",
        "region",
        "month_sin",
        "month_cos",
        "hour_sin",
        "hour_cos",
    ]


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, 99.0, "north"), (2, 4.0, 8.0, 88.0, "south")],
        schema="row_id INT, month DOUBLE, hour DOUBLE, untouched DOUBLE, region STRING",
    )

    model = CyclicalFeatures(variables=["month"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [99.0, 88.0]
    assert [row.region for row in result] == ["north", "south"]
    assert not hasattr(result[0], "hour_sin")


@pytest.mark.parametrize(
    ("variables", "max_values", "message"),
    [
        (["missing"], None, "missing"),
        (["month", "month"], None, "Duplicate"),
        (["month", "region"], None, "numeric"),
        (["month", "hour"], {"month": 12.0}, "hour"),
        (["month", "hour"], {"month": 0.0, "hour": 24.0}, "positive"),
        (["month", "hour"], {"month": 12.0, "hour": 24.0, "extra": 7.0}, "extra"),
    ],
)
def test_invalid_cyclical_feature_configuration_is_rejected(
    spark_session,
    variables,
    max_values,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0, "north")],
        schema="row_id INT, month DOUBLE, hour DOUBLE, region STRING",
    )

    with pytest.raises((TypeError, ValueError), match=message):
        CyclicalFeatures(variables=variables, max_values=max_values).fit(dataset)


def test_cyclical_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 2.0), (2, 4.0, 8.0)], schema="row_id INT, month DOUBLE, hour DOUBLE"
    )

    model = CyclicalFeatures(variables=["month", "hour"]).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "sin" in plan_text.lower()
    assert "cos" in plan_text.lower()
