"""Contract tests for the drop-missing-data transformer."""

from __future__ import annotations

import pytest

from spark_feature_engine.imputation import DropMissingData


def test_drop_missing_data_removes_rows_with_nulls_in_selected_columns_only(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, 1.0, "drop-left"),
            (2, 2.0, None, "drop-right"),
            (3, 3.0, 4.0, None),
            (4, 5.0, 6.0, "keep"),
        ],
        schema="row_id INT, left_value DOUBLE, right_value DOUBLE, untouched STRING",
    )

    result = (
        DropMissingData(variables=["left_value", "right_value"])
        .transform(dataset)
        .orderBy("row_id")
        .collect()
    )

    assert [row.row_id for row in result] == [3, 4]
    assert [row.left_value for row in result] == [3.0, 5.0]
    assert [row.right_value for row in result] == [4.0, 6.0]
    assert [row.untouched for row in result] == [None, "keep"]


def test_drop_missing_data_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 1.0, None, "alpha"),
            (2, 2.0, 2.0, "beta"),
        ],
        schema="row_id INT, selected DOUBLE, optional DOUBLE, label STRING",
    )

    result = (
        DropMissingData(variables=["selected"])
        .transform(dataset)
        .orderBy("row_id")
        .collect()
    )

    assert [row.row_id for row in result] == [1, 2]
    assert [row.optional for row in result] == [None, 2.0]
    assert [row.label for row in result] == ["alpha", "beta"]


def test_drop_missing_data_rejects_missing_target_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 1.0)], schema="row_id INT, selected DOUBLE"
    )

    transformer = DropMissingData(variables=["missing"])

    with pytest.raises((ValueError, KeyError), match="missing"):
        transformer.transform(dataset)
