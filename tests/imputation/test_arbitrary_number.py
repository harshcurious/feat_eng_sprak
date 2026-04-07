"""Contract tests for arbitrary numerical imputation."""

from __future__ import annotations

import pytest

from spark_feature_engine.imputation import ArbitraryNumberImputer


def test_arbitrary_value_fills_selected_numeric_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None, 10.0, "keep"), (2, 2.0, None, "keep"), (3, 4.0, 30.0, "keep")],
        schema="row_id INT, x DOUBLE, y DOUBLE, label STRING",
    )

    transformer = ArbitraryNumberImputer(variables=["x", "y"], fill_value=7.5)

    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.x for row in result] == pytest.approx([7.5, 2.0, 4.0])
    assert [row.y for row in result] == pytest.approx([10.0, 7.5, 30.0])


def test_non_selected_columns_remain_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None, None, "alpha"), (2, 2.0, 100.0, "beta"), (3, 4.0, None, "gamma")],
        schema="row_id INT, x DOUBLE, untouched DOUBLE, label STRING",
    )

    transformer = ArbitraryNumberImputer(variables=["x"], fill_value=3.0)

    result = transformer.transform(dataset).orderBy("row_id").collect()

    assert [row.x for row in result] == pytest.approx([3.0, 2.0, 4.0])
    assert [row.untouched for row in result] == [None, 100.0, None]
    assert [row.label for row in result] == ["alpha", "beta", "gamma"]


@pytest.mark.parametrize(
    ("variables", "error_type", "message"),
    [(["label"], TypeError, "numeric"), (["missing"], ValueError, "missing")],
)
def test_invalid_numeric_columns_are_rejected(
    spark_session,
    variables: list[str],
    error_type: type[Exception],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, None, "alpha")], schema="row_id INT, x DOUBLE, label STRING"
    )

    transformer = ArbitraryNumberImputer(variables=variables, fill_value=1.0)

    with pytest.raises(error_type, match=message):
        transformer.transform(dataset)


def test_param_handling_uses_spark_params() -> None:
    transformer = ArbitraryNumberImputer(variables=["x"], fill_value=3.25)

    assert transformer.isSet(transformer.variables)
    assert transformer.getOrDefault(transformer.variables) == ["x"]
    assert transformer.isSet(transformer.fill_value)
    assert transformer.getOrDefault(transformer.fill_value) == pytest.approx(3.25)

    copied = transformer.copy({transformer.fill_value: 4.5})

    assert copied.getOrDefault(copied.variables) == ["x"]
    assert copied.getOrDefault(copied.fill_value) == pytest.approx(4.5)
    assert transformer.getOrDefault(transformer.fill_value) == pytest.approx(3.25)
