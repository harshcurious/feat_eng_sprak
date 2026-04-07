"""Contract tests for the categorical imputer."""

from __future__ import annotations

import pytest

from spark_feature_engine.imputation import CategoricalImputer


def test_default_fill_value_uses_missing_for_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, "north", 10),
            (2, "paris", None, 20),
            (3, None, "south", 30),
        ],
        schema="row_id INT, city STRING, region STRING, score INT",
    )

    transformer = CategoricalImputer(variables=["city"])
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert transformer.getOrDefault(transformer.fill_value) == "missing"
    assert [row.city for row in result] == ["missing", "paris", "missing"]
    assert [row.region for row in result] == ["north", None, "south"]


def test_custom_fill_value_is_applied_to_selected_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, None, 10),
            (2, "paris", None, 20),
            (3, None, "west", 30),
        ],
        schema="row_id INT, city STRING, region STRING, score INT",
    )

    transformer = CategoricalImputer(
        variables=["city", "region"],
        fill_value="unknown",
    )
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert transformer.getOrDefault(transformer.fill_value) == "unknown"
    assert [row.city for row in result] == ["unknown", "paris", "unknown"]
    assert [row.region for row in result] == ["unknown", "unknown", "west"]
    assert [row.score for row in result] == [10, 20, 30]


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, None, None, "alpha"),
            (2, "paris", None, "beta"),
            (3, None, "west", "gamma"),
        ],
        schema="row_id INT, city STRING, region STRING, label STRING",
    )

    transformer = CategoricalImputer(variables=["city"], fill_value="missing")
    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.city for row in result] == ["missing", "paris", "missing"]
    assert [row.region for row in result] == [None, None, "west"]
    assert [row.label for row in result] == ["alpha", "beta", "gamma"]


@pytest.mark.parametrize(
    ("variables", "error_type", "message"),
    [(["score"], TypeError, "string"), (["missing"], ValueError, "missing")],
)
def test_invalid_categorical_columns_are_rejected(
    spark_session,
    variables: list[str],
    error_type: type[Exception],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None, "alpha")], schema="row_id INT, score DOUBLE, city STRING"
    )

    transformer = CategoricalImputer(variables=variables)

    with pytest.raises(error_type, match=message):
        transformer.transform(dataset)
