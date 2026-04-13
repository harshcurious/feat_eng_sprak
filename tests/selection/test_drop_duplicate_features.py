"""Contract tests for the Spark-native duplicate feature selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.drop_duplicate_features import (
    DropDuplicateFeatures,
    DropDuplicateFeaturesModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_duplicate_columns_are_learned_and_removed_deterministically(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 10, 10, "north"),
            (2, 20, 20, "south"),
            (3, 30, 30, "east"),
        ],
        schema="row_id INT, income INT, income_copy INT, region STRING",
    )

    model = DropDuplicateFeatures().fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, DropDuplicateFeaturesModel)
    assert model.features_to_drop_ == ["income_copy"]
    assert model.duplicated_feature_sets_ == [["income", "income_copy"]]
    assert result[0].asDict() == {"row_id": 1, "income": 10, "region": "north"}
    assert result[1].asDict() == {"row_id": 2, "income": 20, "region": "south"}


def test_duplicate_selector_respects_explicit_variable_selection(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 10, 10, 99),
            (2, 20, 20, 88),
        ],
        schema="row_id INT, income INT, income_copy INT, untouched INT",
    )

    model = DropDuplicateFeatures(variables=["income", "income_copy"]).fit(dataset)

    assert model.variables_ == ["income", "income_copy"]
    assert model.features_to_drop_ == ["income_copy"]
    assert model.transform(dataset).columns == ["row_id", "income", "untouched"]


def test_missing_values_raise_rejects_nulls_in_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10, None), (2, 20, None)],
        schema="row_id INT, income INT, income_copy INT",
    )

    with pytest.raises(ValueError, match="missing"):
        DropDuplicateFeatures(
            variables=["income", "income_copy"],
            missing_values="raise",
        ).fit(dataset)


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10, 10, "north"), (2, 20, 20, "south")],
        schema="row_id INT, income INT, income_copy INT, region STRING",
    )

    model = DropDuplicateFeatures().fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.region for row in result] == ["north", "south"]
    assert [row.row_id for row in result] == [1, 2]


@pytest.mark.parametrize(
    ("variables", "missing_values", "message"),
    [
        (["missing", "income"], "ignore", "missing"),
        (["income", "income"], "ignore", "Duplicate"),
        (["income"], "ignore", "at least 2"),
        (["income", "income_copy"], "mystery", "missing_values"),
    ],
)
def test_invalid_duplicate_selector_configuration_is_rejected(
    spark_session,
    variables,
    missing_values,
    message,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10, 10)], schema="row_id INT, income INT, income_copy INT"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        DropDuplicateFeatures(
            variables=variables,
            missing_values=missing_values,
        ).fit(dataset)


def test_duplicate_selector_uses_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10, 10), (2, 20, 20)],
        schema="row_id INT, income INT, income_copy INT",
    )

    model = DropDuplicateFeatures().fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
