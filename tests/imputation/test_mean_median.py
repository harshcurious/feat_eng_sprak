"""Contract tests for the learned mean/median imputer."""

from __future__ import annotations

import pytest

from spark_feature_engine.imputation.mean_median import (
    MeanMedianImputer,
    MeanMedianImputerModel,
)


def test_mean_strategy_fills_selected_numeric_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, 10.0, "keep"),
            (2, 2.0, None, "keep"),
            (3, 4.0, 30.0, "keep"),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    transformer = MeanMedianImputer(variables=["x", "y"], imputation_method="mean")

    model = transformer.fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, MeanMedianImputerModel)
    assert model.variables_ == ["x", "y"]
    assert model.imputer_dict_ == pytest.approx({"x": 3.0, "y": 20.0})
    assert [row.x for row in result] == pytest.approx([3.0, 2.0, 4.0])
    assert [row.y for row in result] == pytest.approx([10.0, 20.0, 30.0])


def test_median_strategy_fills_selected_numeric_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, 10.0),
            (2, 1.0, None),
            (3, 5.0, 20.0),
            (4, 9.0, 40.0),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE",
    )

    transformer = MeanMedianImputer(variables=["x", "y"], imputation_method="median")

    model = transformer.fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.imputer_dict_ == pytest.approx({"x": 5.0, "y": 20.0})
    assert [row.x for row in result] == pytest.approx([5.0, 1.0, 5.0, 9.0])
    assert [row.y for row in result] == pytest.approx([10.0, 20.0, 20.0, 40.0])


def test_fit_auto_discovers_numeric_variables_when_unset(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, 10.0, "alpha"),
            (2, 2.0, None, "beta"),
            (3, 4.0, 30.0, "gamma"),
        ],
        schema="row_id INT, x DOUBLE, y DOUBLE, city STRING",
    )

    model = MeanMedianImputer(imputation_method="mean").fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.variables_ == ["row_id", "x", "y"]
    assert model.imputer_dict_ == pytest.approx({"row_id": 2.0, "x": 3.0, "y": 20.0})
    assert [row.city for row in result] == ["alpha", "beta", "gamma"]
    assert [row.x for row in result] == pytest.approx([3.0, 2.0, 4.0])
    assert [row.y for row in result] == pytest.approx([10.0, 20.0, 30.0])


def test_explicit_variables_limit_imputation_scope_and_leave_other_columns_unchanged(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, None, "alpha"),
            (2, 2.0, 100.0, "beta"),
            (3, 4.0, None, "gamma"),
        ],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, city STRING",
    )

    model = MeanMedianImputer(variables=["x"], imputation_method="mean").fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.variables_ == ["x"]
    assert model.imputer_dict_ == pytest.approx({"x": 3.0})
    assert [row.x for row in result] == pytest.approx([3.0, 2.0, 4.0])
    assert [row.untouched for row in result] == [None, 100.0, None]
    assert [row.city for row in result] == ["alpha", "beta", "gamma"]


@pytest.mark.parametrize(
    ("variables", "error_type", "message"),
    [(["city"], TypeError, "numeric"), (["missing"], ValueError, "missing")],
)
def test_invalid_target_columns_are_rejected(
    spark_session,
    variables: list[str],
    error_type: type[Exception],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None, "alpha")], schema="row_id INT, x DOUBLE, city STRING"
    )

    transformer = MeanMedianImputer(variables=variables, imputation_method="mean")

    with pytest.raises(error_type, match=message):
        transformer.fit(dataset)


def test_fitted_model_exposes_trailing_underscore_attributes_only(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None), (2, 2.0), (3, 4.0)], schema="row_id INT, x DOUBLE"
    )

    model = MeanMedianImputer(variables=["x"], imputation_method="mean").fit(dataset)

    assert model.variables_ == ["x"]
    assert model.imputer_dict_ == pytest.approx({"x": 3.0})
    assert not hasattr(model, "variables")
    assert not hasattr(model, "imputer_dict")


def test_fitted_model_transforms_with_native_spark_operations_only(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None), (2, 2.0), (3, 4.0)], schema="row_id INT, x DOUBLE"
    )

    model = MeanMedianImputer(variables=["x"], imputation_method="mean").fit(dataset)
    transformed = model.transform(dataset)

    plan_text = "\n".join(
        [
            transformed._jdf.queryExecution().optimizedPlan().toString(),
            transformed._jdf.queryExecution().executedPlan().toString(),
        ]
    )

    assert model.imputer_dict_ == pytest.approx({"x": 3.0})
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
    assert "coalesce" in plan_text.lower() or "fill" in plan_text.lower()
