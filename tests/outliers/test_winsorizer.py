"""Contract tests for the learned Spark-native winsorizer."""

from __future__ import annotations

import pytest

from spark_feature_engine.outliers.winsorizer import (
    Winsorizer,
    WinsorizerModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_learned_caps_clip_selected_values(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, 10.0, "keep"),
            (2, 2.0, 20.0, "keep"),
            (3, 3.0, 30.0, "keep"),
            (4, 100.0, -100.0, "keep"),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    model = Winsorizer(
        variables=["x", "y"], lower_quantile=0.25, upper_quantile=0.75
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, WinsorizerModel)
    assert model.variables_ == ["x", "y"]
    assert model.lower_bounds_ == pytest.approx({"x": 1.5, "y": 15.0})
    assert model.upper_bounds_ == pytest.approx({"x": 2.5, "y": 25.0})
    assert [row.x for row in result] == pytest.approx([1.5, 2.0, 2.5, 2.5])
    assert [row.y for row in result] == pytest.approx([15.0, 20.0, 25.0, 15.0])
    assert [row.label for row in result] == ["keep", "keep", "keep", "keep"]


def test_future_extreme_values_remain_clipped(spark_session) -> None:
    fit_data = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0)], schema="row_id INT, x DOUBLE"
    )
    transform_data = spark_session.createDataFrame(
        [(10, -999.0), (11, 999.0)], schema="row_id INT, x DOUBLE"
    )

    model = Winsorizer(variables=["x"], lower_quantile=0.25, upper_quantile=0.75).fit(
        fit_data
    )
    result = model.transform(transform_data).orderBy("row_id").collect()

    assert model.lower_bounds_ == pytest.approx({"x": 1.5})
    assert model.upper_bounds_ == pytest.approx({"x": 2.5})
    assert [row.x for row in result] == pytest.approx([1.5, 2.5])


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0, 10.0, "alpha"), (2, 2.0, 20.0, "beta")],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    model = Winsorizer(variables=["x"], lower_quantile=0.25, upper_quantile=0.75).fit(
        dataset
    )
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [10.0, 20.0]
    assert [row.label for row in result] == ["alpha", "beta"]


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0)], schema="row_id INT, x DOUBLE"
    )

    model = Winsorizer(variables=["x"], lower_quantile=0.25, upper_quantile=0.75).fit(
        dataset
    )

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

    transformer = Winsorizer(
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
        Winsorizer(
            variables=["x"],
            lower_quantile=lower_quantile,
            upper_quantile=upper_quantile,
        )


def test_winsorizer_transforms_with_native_spark_operations_only(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0), (2, 2.0), (3, 3.0), (4, 100.0)], schema="row_id INT, x DOUBLE"
    )

    model = Winsorizer(variables=["x"], lower_quantile=0.25, upper_quantile=0.75).fit(
        dataset
    )
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.lower_bounds_ == pytest.approx({"x": 1.75})
    assert model.upper_bounds_ == pytest.approx({"x": 27.25})
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "least" in plan_text.lower() or "greatest" in plan_text.lower()
