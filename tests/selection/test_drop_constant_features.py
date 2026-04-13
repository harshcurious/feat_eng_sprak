"""Contract tests for the Spark-native constant feature selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.drop_constant_features import (
    DropConstantFeatures,
    DropConstantFeaturesModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_constant_features_are_learned_and_removed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1, "same", "north"),
            (2, 1, "same", "south"),
            (3, 1, "same", "east"),
        ],
        schema="row_id INT, constant_num INT, constant_str STRING, region STRING",
    )

    model = DropConstantFeatures().fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, DropConstantFeaturesModel)
    assert model.features_to_drop_ == ["constant_num", "constant_str"]
    assert result[0].asDict() == {"row_id": 1, "region": "north"}
    assert result[1].asDict() == {"row_id": 2, "region": "south"}
    assert result[2].asDict() == {"row_id": 3, "region": "east"}


def test_quasi_constant_features_are_removed_by_threshold(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1), (2, 1), (3, 1), (4, 2)], schema="row_id INT, almost_constant INT"
    )

    model = DropConstantFeatures(variables=["almost_constant"], tol=0.75).fit(dataset)

    assert model.features_to_drop_ == ["almost_constant"]
    assert model.transform(dataset).columns == ["row_id"]


def test_missing_values_include_treats_null_as_a_regular_value(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None), (2, None), (3, 2.0), (4, None)], schema="row_id INT, score DOUBLE"
    )

    model = DropConstantFeatures(
        variables=["score"], tol=0.75, missing_values="include"
    ).fit(dataset)

    assert model.features_to_drop_ == ["score"]


def test_missing_values_raise_rejects_nulls(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None), (2, 1.0)], schema="row_id INT, score DOUBLE"
    )

    with pytest.raises(ValueError, match="missing"):
        DropConstantFeatures(variables=["score"], missing_values="raise").fit(dataset)


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1, "same", 10.0), (2, 1, "same", 20.0)],
        schema="row_id INT, constant_num INT, constant_str STRING, untouched DOUBLE",
    )

    model = DropConstantFeatures(variables=["constant_num", "constant_str"]).fit(
        dataset
    )
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.untouched for row in result] == [10.0, 20.0]
    assert [row.row_id for row in result] == [1, 2]


@pytest.mark.parametrize(
    ("variables", "tol", "missing_values", "message"),
    [
        (["missing"], 1.0, "ignore", "missing"),
        (["constant_num", "constant_num"], 1.0, "ignore", "Duplicate"),
        (["constant_num"], -0.1, "ignore", "tol"),
        (["constant_num"], 1.1, "ignore", "tol"),
        (["constant_num"], 1.0, "mystery", "missing_values"),
    ],
)
def test_invalid_constant_selector_configuration_is_rejected(
    spark_session,
    variables,
    tol,
    missing_values,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1, "same")], schema="row_id INT, constant_num INT, constant_str STRING"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        DropConstantFeatures(
            variables=variables,
            tol=tol,
            missing_values=missing_values,
        ).fit(dataset)


def test_selector_rejects_dropping_all_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "same"), (1, "same")], schema="constant_num INT, constant_str STRING"
    )

    with pytest.raises(ValueError, match="all selected features"):
        DropConstantFeatures().fit(dataset)


def test_drop_constant_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1, "same", "north"), (2, 1, "same", "south")],
        schema="row_id INT, constant_num INT, constant_str STRING, region STRING",
    )

    model = DropConstantFeatures().fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
