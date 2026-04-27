"""Validation-focused contract tests for upcoming Phase 6 helpers."""

from __future__ import annotations

import pytest

import spark_feature_engine._validation as validation
from spark_feature_engine.selection._shared import assign_deterministic_folds


def _helper(name: str):
    return getattr(validation, name)


def test_binary_target_validation_accepts_integer_binary_column(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 10.0), (1, 12.0), (0, 14.0)], schema="target INT, value DOUBLE"
    )

    validate_binary_target_column = _helper("validate_binary_target_column")

    assert validate_binary_target_column(dataset, target="target") == "target"


@pytest.mark.parametrize(
    ("rows", "schema", "message"),
    [
        ([(0.0,), (0.5,), (1.0,)], "target DOUBLE", "binary"),
        ([(0,), (1,), (2,)], "target INT", "binary"),
        ([("yes",), ("no",)], "target STRING", "numeric"),
    ],
)
def test_binary_target_validation_rejects_invalid_targets(
    spark_session,
    rows,
    schema: str,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(rows, schema=schema)

    validate_binary_target_column = _helper("validate_binary_target_column")

    with pytest.raises((TypeError, ValueError), match=message):
        validate_binary_target_column(dataset, target="target")


def test_classification_scoring_normalization_accepts_supported_metrics() -> None:
    normalize_classification_scoring = _helper("normalize_classification_scoring")

    assert normalize_classification_scoring(" ROC_AUC ") == "roc_auc"
    assert normalize_classification_scoring("accuracy") == "accuracy"


@pytest.mark.parametrize("value", ["f1", 3])
def test_classification_scoring_normalization_rejects_invalid_metrics(value) -> None:
    normalize_classification_scoring = _helper("normalize_classification_scoring")

    with pytest.raises((TypeError, ValueError), match="scoring"):
        normalize_classification_scoring(value)  # type: ignore[arg-type]


def test_configured_drop_validation_accepts_strict_subset() -> None:
    validate_configured_features_to_drop = _helper(
        "validate_configured_features_to_drop"
    )

    assert validate_configured_features_to_drop(
        dataset_columns=["id", "x1", "x2", "target"],
        features_to_drop=["x2"],
    ) == ["x2"]


@pytest.mark.parametrize(
    ("features_to_drop", "message"),
    [
        (["missing"], "missing"),
        (["x1", "x1"], "Duplicate"),
        (["id", "x1", "x2", "target"], "no columns"),
    ],
)
def test_configured_drop_validation_rejects_invalid_drop_sets(
    features_to_drop,
    message: str,
) -> None:
    validate_configured_features_to_drop = _helper(
        "validate_configured_features_to_drop"
    )

    with pytest.raises(ValueError, match=message):
        validate_configured_features_to_drop(
            dataset_columns=["id", "x1", "x2", "target"],
            features_to_drop=features_to_drop,
        )


class CompatibleClassifier:
    _spark_feature_engine_native = True

    def fit(self, dataset):
        return dataset

    def copy(self, extra=None):
        return self


class CompatibleImportanceClassifier(CompatibleClassifier):
    featureImportances = [0.8, 0.2]


class IncompatibleClassifier:
    pass


def test_native_estimator_validation_accepts_supported_classifier_contract() -> None:
    validate_native_classification_estimator = _helper(
        "validate_native_classification_estimator"
    )

    assert validate_native_classification_estimator(CompatibleClassifier()) is not None


def test_native_estimator_validation_requires_feature_importance_when_requested() -> (
    None
):
    validate_native_classification_estimator = _helper(
        "validate_native_classification_estimator"
    )

    estimator = validate_native_classification_estimator(
        CompatibleImportanceClassifier(),
        require_feature_importance=True,
    )

    assert estimator is not None


@pytest.mark.parametrize(
    ("estimator", "require_feature_importance", "message"),
    [
        (IncompatibleClassifier(), False, "Spark DataFrame-native"),
        (CompatibleClassifier(), True, "feature importance"),
    ],
)
def test_native_estimator_validation_rejects_incompatible_estimators(
    estimator,
    require_feature_importance: bool,
    message: str,
) -> None:
    validate_native_classification_estimator = _helper(
        "validate_native_classification_estimator"
    )

    with pytest.raises(TypeError, match=message):
        validate_native_classification_estimator(
            estimator,
            require_feature_importance=require_feature_importance,
        )


def test_deterministic_fold_assignment_is_stable_for_same_seed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0), (2, 1), (3, 0), (4, 1), (5, 0), (6, 1)],
        schema="id INT, target INT",
    )

    first = assign_deterministic_folds(dataset, n_splits=3, seed=17, order_by=["id"])
    second = assign_deterministic_folds(dataset, n_splits=3, seed=17, order_by=["id"])

    assert (
        first.select("id", "_fold_id").collect()
        == second.select("id", "_fold_id").collect()
    )


def test_deterministic_fold_assignment_changes_with_seed(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0), (2, 1), (3, 0), (4, 1), (5, 0), (6, 1)],
        schema="id INT, target INT",
    )

    first = assign_deterministic_folds(dataset, n_splits=3, seed=17, order_by=["id"])
    second = assign_deterministic_folds(dataset, n_splits=3, seed=99, order_by=["id"])

    assert first.select("_fold_id").collect() != second.select("_fold_id").collect()


@pytest.mark.parametrize("n_splits", [1, True])
def test_deterministic_fold_assignment_requires_valid_split_count(
    spark_session,
    n_splits,
) -> None:
    dataset = spark_session.createDataFrame([(1,)], schema="id INT")

    with pytest.raises(ValueError, match="n_splits"):
        assign_deterministic_folds(dataset, n_splits=n_splits, seed=17, order_by=["id"])
