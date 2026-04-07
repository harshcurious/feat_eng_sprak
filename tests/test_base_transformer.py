"""Contract tests for the shared Spark transformer base."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame

from spark_feature_engine._validation import (
    resolve_variables,
    validate_column_types,
)
from spark_feature_engine.base import (
    BaseSparkEstimator,
    BaseSparkModel,
    BaseSparkTransformer,
)


class DummyBaseTransformer(BaseSparkTransformer):
    """Small concrete transformer for base-contract checks."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset


class DummyBaseEstimator(BaseSparkEstimator):
    """Small concrete estimator for shared base-contract checks."""

    def _fit(self, dataset: DataFrame) -> "DummyBaseModel":
        model = DummyBaseModel()
        model._set_learned_attribute(
            "selected_columns_", self.resolve_variables(dataset)
        )
        return model


class DummyBaseModel(BaseSparkModel):
    """Small concrete fitted model for learned-state contract checks."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("selected_columns_")
        return dataset


def test_param_based_configuration_uses_spark_params() -> None:
    transformer = DummyBaseTransformer(variables=["alpha", "beta"])

    assert transformer.isSet(transformer.variables)
    assert transformer.getOrDefault(transformer.variables) == ["alpha", "beta"]


def test_copy_preserves_params_and_allows_overrides() -> None:
    original = DummyBaseTransformer(variables=["alpha"])

    copied = original.copy({original.variables: ["beta"]})

    assert isinstance(copied, DummyBaseTransformer)
    assert copied is not original
    assert copied.getOrDefault(copied.variables) == ["beta"]
    assert original.getOrDefault(original.variables) == ["alpha"]


def test_variable_resolution_defaults_to_all_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="id INT, label STRING, score DOUBLE"
    )
    transformer = DummyBaseTransformer()

    assert transformer.resolve_variables(dataset) == ["id", "label", "score"]


def test_variable_resolution_respects_explicit_selection(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="id INT, label STRING, score DOUBLE"
    )
    transformer = DummyBaseTransformer(variables=["score", "id"])

    assert transformer.resolve_variables(dataset) == ["score", "id"]


def test_validation_fails_for_missing_variables(spark_session) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")
    transformer = DummyBaseTransformer(variables=["missing"])

    with pytest.raises((ValueError, KeyError), match="missing"):
        transformer.resolve_variables(dataset)


def test_validation_fails_for_invalid_param_type() -> None:
    with pytest.raises((TypeError, ValueError), match="variables"):
        DummyBaseTransformer(variables="alpha")  # type: ignore[arg-type]


def test_base_estimator_shares_param_configuration_helpers() -> None:
    estimator = DummyBaseEstimator(variables=["alpha", "beta"])

    assert estimator.isSet(estimator.variables)
    assert estimator.getOrDefault(estimator.variables) == ["alpha", "beta"]


def test_shared_validation_fails_for_duplicate_variables(spark_session) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")

    with pytest.raises(ValueError, match="Duplicate"):
        resolve_variables(dataset, variables=["id", "id"])


def test_shared_validation_rejects_non_numeric_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")

    with pytest.raises(TypeError, match="numeric"):
        validate_column_types(dataset, ["label"], expected_type="numeric")


def test_shared_validation_accepts_string_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")

    validate_column_types(dataset, ["label"], expected_type="string")


def test_learned_state_uses_trailing_underscore_names() -> None:
    transformer = DummyBaseTransformer()
    transformer._set_learned_attribute("selected_columns_", ["alpha"])
    transformer._set_learned_attribute("schema_snapshot_", "id INT, label STRING")

    assert hasattr(transformer, "selected_columns_")
    assert hasattr(transformer, "schema_snapshot_")
    assert not hasattr(transformer, "selected_columns")
    assert not hasattr(transformer, "schema_snapshot")
    assert all(name.endswith("_") for name in ("selected_columns_", "schema_snapshot_"))


def test_learned_state_rejects_non_trailing_underscore_names() -> None:
    transformer = DummyBaseTransformer()

    with pytest.raises(ValueError, match="trailing underscore"):
        transformer._set_learned_attribute("selected_columns", ["alpha"])


def test_fitted_models_can_assert_required_learned_state(spark_session) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")
    model = DummyBaseEstimator(variables=["label"]).fit(dataset)

    assert model.transform(dataset).columns == dataset.columns


def test_fitted_models_reject_transform_before_learned_state_exists(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame([(1, "x")], schema="id INT, label STRING")
    model = DummyBaseModel()

    with pytest.raises(ValueError, match="selected_columns_"):
        model.transform(dataset)
