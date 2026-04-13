"""Validation-focused contract tests for upcoming Phase 4 helpers."""

from __future__ import annotations

import pytest

import spark_feature_engine._validation as validation


def _helper(name: str):
    return getattr(validation, name)


def test_creation_function_normalization_accepts_supported_values() -> None:
    normalize_functions = _helper("normalize_creation_functions")

    assert normalize_functions(
        ["SUM", " mean "],
        allowed=["sum", "mean", "min", "max", "prod"],
    ) == ["sum", "mean"]


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (["sum", "sum"], "Duplicate"),
        (["sum", "median"], "median"),
        ("sum", "sequence"),
    ],
)
def test_creation_function_normalization_rejects_invalid_values(
    value, message: str
) -> None:
    normalize_functions = _helper("normalize_creation_functions")

    with pytest.raises((TypeError, ValueError), match=message):
        normalize_functions(
            value,  # type: ignore[arg-type]
            allowed=["sum", "mean", "min", "max", "prod"],
        )


def test_minimum_variable_count_validation_rejects_too_few_columns() -> None:
    validate_minimum_count = _helper("validate_minimum_variable_count")

    with pytest.raises(ValueError, match="at least 2"):
        validate_minimum_count(["x1"], minimum=2, name="variables")


def test_relative_variable_resolution_defaults_to_numeric_non_reference_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.5, 3.0, "north")],
        schema="row_id INT, score DOUBLE, visits DOUBLE, region STRING",
    )

    resolve_relative_variables = _helper("resolve_relative_feature_variables")

    assert resolve_relative_variables(
        dataset, variables=None, reference=["visits"]
    ) == [
        "row_id",
        "score",
    ]


def test_relative_variable_resolution_rejects_unknown_reference_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 2.5, 3.0)],
        schema="row_id INT, score DOUBLE, visits DOUBLE",
    )

    resolve_relative_variables = _helper("resolve_relative_feature_variables")

    with pytest.raises(ValueError, match="missing"):
        resolve_relative_variables(dataset, variables=None, reference=["missing"])


def test_max_value_normalization_accepts_positive_numeric_mapping() -> None:
    normalize_max_values = _helper("normalize_max_values")

    assert normalize_max_values(
        {"month": 12, "day": 7.0},
        variables=["month", "day"],
    ) == {"month": 12.0, "day": 7.0}


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        ({"month": 12}, "day"),
        ({"month": 0, "day": 7}, "positive"),
        ({"month": 12, "day": "weekly"}, "numeric"),
        ({"month": 12, "day": 7, "extra": 9}, "extra"),
    ],
)
def test_max_value_normalization_rejects_invalid_mappings(
    mapping, message: str
) -> None:
    normalize_max_values = _helper("normalize_max_values")

    with pytest.raises((TypeError, ValueError), match=message):
        normalize_max_values(mapping, variables=["month", "day"])  # type: ignore[arg-type]
