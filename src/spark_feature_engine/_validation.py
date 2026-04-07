"""Shared schema, option, and learned-state validation helpers."""

from __future__ import annotations

from numbers import Real
from typing import Iterable, Literal, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StringType, StructField

ColumnExpectation = Literal["any", "numeric", "string"]


def to_optional_list_of_strings(value: Sequence[str] | None) -> list[str] | None:
    """Normalize an optional sequence of column names."""
    if value is None:
        return None
    if isinstance(value, str):
        raise TypeError("variables must be a sequence of column names, not a string")

    converted = list(value)
    if any(not isinstance(item, str) for item in converted):
        raise TypeError("variables must contain only string column names")
    return converted


def validate_column_presence(dataset: DataFrame, columns: Sequence[str]) -> None:
    """Fail if any configured columns are missing from the dataset."""
    available = set(dataset.columns)
    missing = [column for column in columns if column not in available]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Unknown variable(s): {joined}")


def validate_unique_columns(columns: Sequence[str]) -> None:
    """Fail if the same target column is configured more than once."""
    seen: set[str] = set()
    duplicates: list[str] = []
    for column in columns:
        if column in seen and column not in duplicates:
            duplicates.append(column)
        seen.add(column)

    if duplicates:
        joined = ", ".join(duplicates)
        raise ValueError(f"Duplicate variable(s) are not allowed: {joined}")


def validate_column_types(
    dataset: DataFrame,
    columns: Sequence[str],
    *,
    expected_type: ColumnExpectation = "any",
) -> None:
    """Fail if configured columns do not match the expected data type."""
    if expected_type == "any":
        return

    schema = {field.name: field for field in dataset.schema.fields}
    invalid: list[str] = []
    for column in columns:
        field = schema[column]
        if not matches_expected_type(field, expected_type):
            invalid.append(f"{column} ({field.dataType.simpleString()})")

    if invalid:
        label = "numeric" if expected_type == "numeric" else "string"
        joined = ", ".join(invalid)
        raise TypeError(f"Expected {label} column(s): {joined}")


def resolve_variables(
    dataset: DataFrame,
    *,
    variables: Sequence[str] | None = None,
    expected_type: ColumnExpectation = "any",
) -> list[str]:
    """Resolve configured variables against a dataset schema."""
    normalized = to_optional_list_of_strings(variables)
    resolved = list(dataset.columns) if normalized is None else normalized

    validate_column_presence(dataset, resolved)
    validate_unique_columns(resolved)
    validate_column_types(dataset, resolved, expected_type=expected_type)
    return resolved


def resolve_categorical_columns(
    dataset: DataFrame,
    *,
    variables: Sequence[str] | None = None,
) -> list[str]:
    """Resolve categorical columns, defaulting to all string columns."""
    normalized = to_optional_list_of_strings(variables)
    if normalized is None:
        resolved = [
            field.name
            for field in dataset.schema.fields
            if matches_expected_type(field, "string")
        ]
    else:
        resolved = normalized

    validate_column_presence(dataset, resolved)
    validate_unique_columns(resolved)
    validate_column_types(dataset, resolved, expected_type="string")
    return resolved


def resolve_numeric_columns(
    dataset: DataFrame,
    *,
    variables: Sequence[str] | None = None,
) -> list[str]:
    """Resolve numeric columns, defaulting to all numeric columns."""
    normalized = to_optional_list_of_strings(variables)
    if normalized is None:
        resolved = [
            field.name
            for field in dataset.schema.fields
            if matches_expected_type(field, "numeric")
        ]
    else:
        resolved = normalized

    validate_column_presence(dataset, resolved)
    validate_unique_columns(resolved)
    validate_column_types(dataset, resolved, expected_type="numeric")
    return resolved


def normalize_option_value(name: str, value: str) -> str:
    """Normalize option-like string parameters deterministically."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value.strip().lower()


def validate_supported_option(
    name: str,
    value: str,
    *,
    allowed: Sequence[str],
) -> str:
    """Normalize an option value and validate it against allowed choices."""
    normalized = normalize_option_value(name, value)
    normalized_allowed = [normalize_option_value(name, option) for option in allowed]
    if normalized not in normalized_allowed:
        joined = ", ".join(repr(option) for option in normalized_allowed)
        raise ValueError(f"Unsupported {name}: {value!r}. Expected one of: {joined}")
    return normalized


def validate_generated_column_names(
    dataset: DataFrame,
    generated_columns: Sequence[str],
    *,
    ignore_existing: Iterable[str] = (),
) -> list[str]:
    """Reject duplicate generated names and collisions with dataset columns."""
    normalized = to_optional_list_of_strings(generated_columns)
    assert normalized is not None

    validate_unique_columns(normalized)

    ignored = set(ignore_existing)
    collisions = [
        column
        for column in normalized
        if column in dataset.columns and column not in ignored
    ]
    if collisions:
        joined = ", ".join(collisions)
        raise ValueError(
            f"Generated column name(s) collide with existing dataset columns: {joined}"
        )

    return normalized


def validate_learned_attribute_name(name: str) -> str:
    """Validate the public naming convention for learned state."""
    if not isinstance(name, str) or not name:
        raise TypeError("Learned attribute names must be non-empty strings")
    if not name.endswith("_"):
        raise ValueError("Learned attributes must use trailing underscore names")
    return name


def validate_fitted_attributes(
    instance: object, attribute_names: Sequence[str]
) -> None:
    """Ensure learned-state attributes exist on a fitted object."""
    missing: list[str] = []
    for name in attribute_names:
        validated_name = validate_learned_attribute_name(name)
        if not hasattr(instance, validated_name):
            missing.append(validated_name)

    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            f"This transformer is not fitted yet. Missing learned attribute(s): {joined}"
        )


def validate_bin_count(bin_count: int) -> int:
    """Validate the configured discretisation bin count."""
    if not isinstance(bin_count, int) or isinstance(bin_count, bool):
        raise TypeError("bin_count must be an integer greater than 1")
    if bin_count <= 1:
        raise ValueError("bin_count must be greater than 1")
    return bin_count


def validate_discretisation_boundaries(
    boundaries: Sequence[float],
    *,
    name: str = "boundaries",
    minimum_size: int = 2,
    allow_infinite: bool = True,
) -> list[float]:
    """Validate ordered numeric boundary values for discretisation."""
    if isinstance(boundaries, (str, bytes)):
        raise TypeError(f"{name} must be a sequence of numeric boundary values")

    normalized = list(boundaries)
    if len(normalized) < minimum_size:
        raise ValueError(
            f"{name} must contain at least {minimum_size} ordered boundary values"
        )

    converted: list[float] = []
    for boundary in normalized:
        if not isinstance(boundary, Real) or isinstance(boundary, bool):
            raise TypeError(f"{name} must contain only numeric boundary values")

        converted_boundary = float(boundary)
        if not allow_infinite and converted_boundary in (float("inf"), float("-inf")):
            raise ValueError(f"{name} cannot contain infinite boundary values")
        converted.append(converted_boundary)

    for previous, current in zip(converted, converted[1:]):
        if current <= previous:
            raise ValueError(f"{name} must be strictly increasing")

    return converted


def matches_expected_type(field: StructField, expected_type: ColumnExpectation) -> bool:
    """Return whether a schema field satisfies the requested expectation."""
    if expected_type == "numeric":
        return isinstance(field.dataType, NumericType)
    if expected_type == "string":
        return isinstance(field.dataType, StringType)
    return True


__all__ = (
    "ColumnExpectation",
    "matches_expected_type",
    "normalize_option_value",
    "resolve_variables",
    "resolve_categorical_columns",
    "resolve_numeric_columns",
    "to_optional_list_of_strings",
    "validate_bin_count",
    "validate_column_presence",
    "validate_column_types",
    "validate_discretisation_boundaries",
    "validate_fitted_attributes",
    "validate_generated_column_names",
    "validate_learned_attribute_name",
    "validate_supported_option",
    "validate_unique_columns",
)
