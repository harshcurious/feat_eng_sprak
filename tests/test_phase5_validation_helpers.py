"""Validation-focused contract tests for upcoming Phase 5 helpers."""

from __future__ import annotations

import pytest

import spark_feature_engine._validation as validation


def _helper(name: str):
    return getattr(validation, name)


def test_selector_threshold_normalization_accepts_probability_values() -> None:
    normalize_threshold = _helper("normalize_selector_threshold")

    assert normalize_threshold(0.8, name="threshold") == pytest.approx(0.8)
    assert normalize_threshold(1.0, name="tol") == pytest.approx(1.0)


@pytest.mark.parametrize("value", [-0.1, 1.1, "high"])
def test_selector_threshold_normalization_rejects_invalid_values(value) -> None:
    normalize_threshold = _helper("normalize_selector_threshold")

    with pytest.raises((TypeError, ValueError), match="threshold"):
        normalize_threshold(value, name="threshold")  # type: ignore[arg-type]


def test_selection_method_normalization_accepts_supported_values() -> None:
    normalize_method = _helper("normalize_selection_method")

    assert (
        normalize_method(
            " Variance ",
            allowed=["missing_values", "cardinality", "variance"],
        )
        == "variance"
    )


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("model_performance", "model_performance"),
        (3, "selection_method"),
    ],
)
def test_selection_method_normalization_rejects_invalid_values(
    value, message: str
) -> None:
    normalize_method = _helper("normalize_selection_method")

    with pytest.raises((TypeError, ValueError), match=message):
        normalize_method(
            value,  # type: ignore[arg-type]
            allowed=["missing_values", "cardinality", "variance"],
        )


def test_selection_numeric_resolution_defaults_to_all_numeric_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, 4.0, "north")],
        schema="row_id INT, income DOUBLE, baseline DOUBLE, region STRING",
    )

    resolve_numeric_selection_columns = _helper("resolve_numeric_selection_columns")

    assert resolve_numeric_selection_columns(dataset) == [
        "row_id",
        "income",
        "baseline",
    ]


def test_selection_numeric_resolution_requires_multiple_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 10.0, "north")], schema="row_id INT, income DOUBLE, region STRING"
    )

    resolve_numeric_selection_columns = _helper("resolve_numeric_selection_columns")

    with pytest.raises(ValueError, match="at least 2"):
        resolve_numeric_selection_columns(dataset, variables=["income"])


def test_features_to_drop_validation_accepts_strict_subset() -> None:
    validate_features_to_drop = _helper("validate_features_to_drop")

    assert validate_features_to_drop(
        variables=["a", "b", "c"],
        features_to_drop=["b"],
    ) == ["b"]


@pytest.mark.parametrize(
    ("features_to_drop", "message"),
    [
        (["missing"], "missing"),
        (["a", "a"], "Duplicate"),
        (["a", "b", "c"], "all selected features"),
    ],
)
def test_features_to_drop_validation_rejects_invalid_drop_sets(
    features_to_drop, message: str
) -> None:
    validate_features_to_drop = _helper("validate_features_to_drop")

    with pytest.raises(ValueError, match=message):
        validate_features_to_drop(
            variables=["a", "b", "c"],
            features_to_drop=features_to_drop,
        )
