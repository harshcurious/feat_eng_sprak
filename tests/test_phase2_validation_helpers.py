"""Validation-focused contract tests for upcoming Phase 2 helpers."""

from __future__ import annotations

import pytest

import spark_feature_engine._validation as validation


def _helper(name: str):
    return getattr(validation, name)


def test_categorical_resolution_defaults_to_all_string_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "north", 2.5)],
        schema="row_id INT, city STRING, region STRING, score DOUBLE",
    )

    resolver = _helper("resolve_categorical_columns")

    assert resolver(dataset) == ["city", "region"]


def test_numeric_resolution_defaults_to_all_numeric_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 2.5, 8)],
        schema="row_id INT, city STRING, score DOUBLE, visits INT",
    )

    resolver = _helper("resolve_numeric_columns")

    assert resolver(dataset) == ["row_id", "score", "visits"]


def test_categorical_resolution_respects_explicit_selection(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "north", 2.5)],
        schema="row_id INT, city STRING, region STRING, score DOUBLE",
    )

    resolver = _helper("resolve_categorical_columns")

    assert resolver(dataset, variables=["region"]) == ["region"]


@pytest.mark.parametrize(
    ("helper_name", "variables", "message"),
    [
        ("resolve_categorical_columns", ["score"], "string"),
        ("resolve_numeric_columns", ["city"], "numeric"),
        ("resolve_categorical_columns", ["city", "city"], "Duplicate"),
        ("resolve_numeric_columns", ["score", "score"], "Duplicate"),
    ],
)
def test_phase2_selected_column_helpers_reject_invalid_targets(
    spark_session,
    helper_name: str,
    variables: list[str],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 2.5)], schema="row_id INT, city STRING, score DOUBLE"
    )

    resolver = _helper(helper_name)

    with pytest.raises((TypeError, ValueError), match=message):
        resolver(dataset, variables=variables)


def test_generated_column_collision_helper_rejects_existing_dataset_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "red", 1)], schema="row_id INT, color STRING, color_red INT"
    )

    validate_generated_columns = _helper("validate_generated_column_names")

    with pytest.raises(ValueError, match="color_red"):
        validate_generated_columns(dataset, ["color_red", "color_blue"])


def test_generated_column_collision_helper_rejects_duplicate_generated_names(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "red")], schema="row_id INT, color STRING"
    )

    validate_generated_columns = _helper("validate_generated_column_names")

    with pytest.raises(ValueError, match="color_red"):
        validate_generated_columns(dataset, ["color_red", "color_red"])


@pytest.mark.parametrize(
    ("name", "value", "allowed", "expected_message"),
    [
        ("unseen_policy", "mystery", ["ignore", "encode", "raise"], "mystery"),
        ("method", "dense", ["count", "frequency"], "dense"),
        ("output", "labels", ["bin", "boundaries"], "labels"),
    ],
)
def test_unsupported_phase2_policy_values_are_rejected(
    name: str,
    value: str,
    allowed: list[str],
    expected_message: str,
) -> None:
    validate_supported_option = _helper("validate_supported_option")

    with pytest.raises(ValueError, match=expected_message):
        validate_supported_option(name, value, allowed=allowed)


@pytest.mark.parametrize("bin_count", [0, 1, -3])
def test_invalid_bin_count_configuration_is_rejected(bin_count: int) -> None:
    validate_bin_count = _helper("validate_bin_count")

    with pytest.raises(ValueError, match="bin"):
        validate_bin_count(bin_count)
