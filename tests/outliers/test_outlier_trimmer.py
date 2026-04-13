"""Contract tests for the learned Spark-native outlier trimmer."""

from __future__ import annotations

import pytest

from spark_feature_engine.outliers.outlier_trimmer import (
    OutlierTrimmer,
    OutlierTrimmerModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_learned_trimming_bounds_exposed_on_fitted_model(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 10.0, "keep"),
            (2, 2.0, 20.0, "keep"),
            (3, 3.0, 30.0, "keep"),
            (4, 100.0, -100.0, "keep"),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    model = OutlierTrimmer(
        variables=["x", "y"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)

    assert isinstance(model, OutlierTrimmerModel)
    assert model.variables_ == ["x", "y"]
    assert model.lower_bounds_ == pytest.approx({"x": 1.5, "y": 15.0})
    assert model.upper_bounds_ == pytest.approx({"x": 2.5, "y": 25.0})
    assert model.lower_quantile_ == pytest.approx(0.25)
    assert model.upper_quantile_ == pytest.approx(0.75)


def test_rows_outside_learned_bounds_are_removed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 10.0, "keep"),
            (2, 2.0, 20.0, "keep"),
            (3, 3.0, 30.0, "keep"),
            (4, 100.0, -100.0, "drop"),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    model = OutlierTrimmer(
        variables=["x", "y"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [2]
    assert [row.label for row in result] == ["keep"]


def test_single_column_rows_outside_bounds_are_removed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0)],
        schema="row_id INT, x DOUBLE",
    )

    model = OutlierTrimmer(
        variables=["x"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [2]


def test_schema_is_preserved_after_row_removal(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 10.0, "alpha"), (2, 2.0, 20.0, "beta")],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    model = OutlierTrimmer(
        variables=["x"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    result = model.transform(dataset)

    assert result.schema.simpleString() == dataset.schema.simpleString()


def test_non_selected_columns_remain_unchanged_before_filtering(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 10.0, "alpha"),
            (2, 2.0, 20.0, "beta"),
            (3, 3.0, 30.0, "gamma"),
            (4, 100.0, 40.0, "drop"),
        ],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    model = OutlierTrimmer(
        variables=["x"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [20.0]
    assert [row.label for row in result] == ["beta"]


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0)], schema="row_id INT, x DOUBLE"
    )

    model = OutlierTrimmer(
        variables=["x"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)

    assert model.variables_ == ["x"]
    assert model.lower_bounds_ == pytest.approx({"x": 1.5})
    assert model.upper_bounds_ == pytest.approx({"x": 2.5})
    assert not hasattr(model, "variables")
    assert not hasattr(model, "lower_bounds")
    assert not hasattr(model, "upper_bounds")


@pytest.mark.parametrize(
    ("variables", "message"),
    [
        (["missing"], "missing"),
        (["x", "x"], "Duplicate"),
    ],
)
def test_invalid_configuration_is_rejected(
    spark_session, variables: list[str], message: str
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, "alpha")], schema="row_id INT, x DOUBLE, label STRING"
    )

    transformer = OutlierTrimmer(
        variables=variables, lower_quantile=0.25, upper_quantile=0.75
    )

    with pytest.raises((ValueError, KeyError, TypeError), match=message):
        transformer.fit(dataset)


@pytest.mark.parametrize(
    ("lower_quantile", "upper_quantile", "message"),
    [(-0.1, 0.9, "lower"), (0.1, 1.5, "upper"), (0.9, 0.1, "order")],
)
def test_unsupported_parameter_values_are_rejected(
    lower_quantile: float, upper_quantile: float, message: str
) -> None:
    with pytest.raises((ValueError, TypeError), match=message):
        OutlierTrimmer(
            variables=["x"],
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
        )


def test_outlier_trimmer_uses_native_spark_operations_only(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0)], schema="row_id INT, x DOUBLE"
    )

    model = OutlierTrimmer(
        variables=["x"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.lower_bounds_ == pytest.approx({"x": 1.5})
    assert model.upper_bounds_ == pytest.approx({"x": 2.5})
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "filter" in plan_text.lower() or "where" in plan_text.lower()
