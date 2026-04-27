"""Contract tests for the Spark-native configured feature dropper."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.drop_features import DropFeatures


def test_drop_features_removes_configured_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 1, "north"), (2, 20.0, 0, "south")],
        schema="row_id INT, amount DOUBLE, target INT, region STRING",
    )

    transformed = DropFeatures(features_to_drop=["amount", "target"]).transform(dataset)

    assert transformed.columns == ["row_id", "region"]


def test_drop_features_preserves_non_selected_columns_and_row_order(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 1, "north"), (2, 20.0, 0, "south")],
        schema="row_id INT, amount DOUBLE, target INT, region STRING",
    )

    rows = DropFeatures(features_to_drop=["amount"]).transform(dataset).collect()

    assert [row.asDict() for row in rows] == [
        {"row_id": 1, "target": 1, "region": "north"},
        {"row_id": 2, "target": 0, "region": "south"},
    ]


@pytest.mark.parametrize(
    ("features_to_drop", "message"),
    [
        (["missing"], "missing"),
        (["amount", "amount"], "Duplicate"),
        (["row_id", "amount", "target", "region"], "no columns"),
    ],
)
def test_drop_features_rejects_invalid_configuration(
    spark_session,
    features_to_drop,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 1, "north")],
        schema="row_id INT, amount DOUBLE, target INT, region STRING",
    )

    with pytest.raises(ValueError, match=message):
        DropFeatures(features_to_drop=features_to_drop).transform(dataset)


def test_drop_features_rejects_non_sequence_string_configuration(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0)], schema="row_id INT, amount DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match="features_to_drop"):
        DropFeatures(features_to_drop="amount").transform(dataset)  # type: ignore[arg-type]
