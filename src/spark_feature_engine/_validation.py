"""Shared schema, option, and learned-state validation helpers."""

from __future__ import annotations

from numbers import Real
from typing import Iterable, Literal, Mapping, Sequence, SupportsFloat

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


def discover_numeric_columns(dataset: DataFrame) -> list[str]:
    """Return all numeric columns in schema order."""
    return resolve_numeric_columns(dataset)


def normalize_option_value(name: str, value: str) -> str:
    """Normalize option-like string parameters deterministically."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value.strip().lower()


def normalize_exponent(value: Real) -> float:
    """Normalize a power-transform exponent."""
    if not isinstance(value, Real) or isinstance(value, bool):
        raise TypeError("exponent must be a real number")
    return float(value)


def normalize_creation_functions(
    value: Sequence[str], *, allowed: Sequence[str], name: str = "func"
) -> list[str]:
    """Normalize a sequence of supported creation-function names."""
    if isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence of supported function names")

    normalized = to_optional_list_of_strings(value)
    assert normalized is not None
    if not normalized:
        raise ValueError(f"{name} must contain at least one function name")

    converted = [normalize_option_value(name, item) for item in normalized]
    validate_unique_columns(converted)

    normalized_allowed = [normalize_option_value(name, option) for option in allowed]
    invalid = [item for item in converted if item not in normalized_allowed]
    if invalid:
        joined = ", ".join(invalid)
        raise ValueError(f"Unsupported {name}: {joined}")

    return converted


def validate_minimum_variable_count(
    columns: Sequence[str], *, minimum: int, name: str = "variables"
) -> list[str]:
    """Ensure a column collection contains at least a minimum count."""
    normalized = to_optional_list_of_strings(columns)
    assert normalized is not None

    if not isinstance(minimum, int) or isinstance(minimum, bool) or minimum <= 0:
        raise ValueError("minimum must be a positive integer")
    if len(normalized) < minimum:
        raise ValueError(f"{name} must contain at least {minimum} column names")
    return normalized


def resolve_relative_feature_variables(
    dataset: DataFrame,
    *,
    variables: Sequence[str] | None = None,
    reference: Sequence[str],
) -> list[str]:
    """Resolve relative-feature variables, defaulting to numeric non-reference columns."""
    resolved_reference = resolve_numeric_columns(dataset, variables=reference)
    if variables is None:
        resolved_variables = [
            column
            for column in resolve_numeric_columns(dataset)
            if column not in resolved_reference
        ]
    else:
        resolved_variables = resolve_numeric_columns(dataset, variables=variables)

    return validate_minimum_variable_count(
        resolved_variables,
        minimum=1,
        name="variables",
    )


def normalize_max_values(
    max_values: Mapping[str, Real], *, variables: Sequence[str]
) -> dict[str, float]:
    """Validate a per-variable positive numeric maximum-value mapping."""
    if not isinstance(max_values, Mapping):
        raise TypeError(
            "max_values must be a mapping of variable names to numeric values"
        )

    resolved_variables = to_optional_list_of_strings(variables)
    assert resolved_variables is not None
    validate_unique_columns(resolved_variables)

    if any(not isinstance(key, str) for key in max_values):
        raise TypeError("max_values keys must be string variable names")

    expected = set(resolved_variables)
    provided = set(max_values)
    missing = [variable for variable in resolved_variables if variable not in provided]
    extra = sorted(provided - expected)
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"max_values is missing variable(s): {joined}")
    if extra:
        joined = ", ".join(extra)
        raise ValueError(f"max_values contains unknown variable(s): {joined}")

    normalized: dict[str, float] = {}
    for variable in resolved_variables:
        value = max_values[variable]
        if not isinstance(value, Real) or isinstance(value, bool):
            raise TypeError("max_values must contain only numeric values")
        converted = float(value)
        if converted <= 0:
            raise ValueError("max_values must contain only positive values")
        normalized[variable] = converted

    return normalized


def normalize_selector_threshold(
    value: SupportsFloat, *, name: str = "threshold"
) -> float:
    """Normalize a selector threshold constrained to the inclusive [0, 1] range."""
    if isinstance(value, bool):
        raise TypeError(f"{name} must be a real number between 0 and 1")
    try:
        converted = float(value)
    except (TypeError, ValueError) as error:
        raise TypeError(f"{name} must be a real number between 0 and 1") from error
    if converted < 0 or converted > 1:
        raise ValueError(f"{name} must be a real number between 0 and 1")
    return converted


def normalize_selection_method(value: str, *, allowed: Sequence[str]) -> str:
    """Normalize and validate a selection strategy name."""
    return validate_supported_option("selection_method", value, allowed=allowed)


def resolve_numeric_selection_columns(
    dataset: DataFrame,
    *,
    variables: Sequence[str] | None = None,
    minimum: int = 2,
) -> list[str]:
    """Resolve numeric selector columns and require a minimum count."""
    resolved = resolve_numeric_columns(dataset, variables=variables)
    return validate_minimum_variable_count(
        resolved,
        minimum=minimum,
        name="variables",
    )


def validate_features_to_drop(
    *,
    variables: Sequence[str],
    features_to_drop: Sequence[str],
) -> list[str]:
    """Validate a learned selector drop set against selected variables."""
    resolved_variables = to_optional_list_of_strings(variables)
    drop_candidates = to_optional_list_of_strings(features_to_drop)
    assert resolved_variables is not None
    assert drop_candidates is not None

    validate_unique_columns(resolved_variables)
    validate_unique_columns(drop_candidates)

    unknown = [column for column in drop_candidates if column not in resolved_variables]
    if unknown:
        joined = ", ".join(unknown)
        raise ValueError(
            f"features_to_drop contains unknown selected variable(s): {joined}"
        )
    if len(drop_candidates) >= len(resolved_variables):
        raise ValueError(
            "Selector cannot drop all selected features; at least one selected feature must remain"
        )
    return drop_candidates


def validate_positive_values(
    values: Sequence[Real], *, name: str = "values"
) -> list[float]:
    """Validate strictly positive numeric values."""
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{name} must be a sequence of numeric values")

    converted: list[float] = []
    for value in values:
        if not isinstance(value, Real) or isinstance(value, bool):
            raise TypeError(f"{name} must contain only numeric values")
        converted_value = float(value)
        if converted_value <= 0:
            raise ValueError(f"{name} must contain only positive values")
        converted.append(converted_value)
    return converted


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


def validate_learned_state(instance: object, attribute_names: Sequence[str]) -> None:
    """Alias for validating fitted learned-state attributes."""
    validate_fitted_attributes(instance, attribute_names)


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


def validate_outlier_bounds(
    bounds: Sequence[float],
    *,
    name: str = "bounds",
) -> list[float]:
    """Validate learned outlier bounds for a selected numeric column."""
    return validate_discretisation_boundaries(
        bounds,
        name=name,
        minimum_size=2,
        allow_infinite=False,
    )


def matches_expected_type(field: StructField, expected_type: ColumnExpectation) -> bool:
    """Return whether a schema field satisfies the requested expectation."""
    if expected_type == "numeric":
        return isinstance(field.dataType, NumericType)
    if expected_type == "string":
        return isinstance(field.dataType, StringType)
    return True


__all__ = (
    "ColumnExpectation",
    "discover_numeric_columns",
    "matches_expected_type",
    "normalize_creation_functions",
    "normalize_option_value",
    "normalize_max_values",
    "normalize_exponent",
    "normalize_selection_method",
    "normalize_selector_threshold",
    "resolve_variables",
    "resolve_categorical_columns",
    "resolve_numeric_columns",
    "resolve_numeric_selection_columns",
    "resolve_relative_feature_variables",
    "to_optional_list_of_strings",
    "validate_bin_count",
    "validate_column_presence",
    "validate_column_types",
    "validate_discretisation_boundaries",
    "validate_fitted_attributes",
    "validate_generated_column_names",
    "validate_learned_attribute_name",
    "validate_learned_state",
    "validate_minimum_variable_count",
    "validate_outlier_bounds",
    "validate_positive_values",
    "validate_features_to_drop",
    "validate_supported_option",
    "validate_unique_columns",
)
