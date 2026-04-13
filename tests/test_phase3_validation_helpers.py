"""Phase 3 foundation contract tests for shared validation helpers."""

from __future__ import annotations

import pytest

from spark_feature_engine._validation import (
    resolve_variables,
    validate_column_types,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkModel, BaseSparkTransformer


class DummyPhase3Transformer(BaseSparkTransformer):
    def _transform(self, dataset):
        return dataset


class DummyPhase3Model(BaseSparkModel):
    def _transform(self, dataset):
        self.require_fitted("selected_columns_")
        return dataset


def _plan_text(dataset) -> str:
    return dataset._jdf.queryExecution().executedPlan().toString()


def test_phase3_selected_column_resolution_defaults_to_all_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="row_id INT, label STRING, score DOUBLE"
    )

    assert resolve_variables(dataset) == ["row_id", "label", "score"]


def test_phase3_numeric_type_validation_rejects_string_columns(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="row_id INT, label STRING, score DOUBLE"
    )

    with pytest.raises(TypeError, match="numeric"):
        validate_column_types(dataset, ["label"], expected_type="numeric")


def test_phase3_duplicate_targets_are_rejected(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="row_id INT, label STRING, score DOUBLE"
    )

    with pytest.raises(ValueError, match="Duplicate"):
        resolve_variables(dataset, variables=["score", "score"])


def test_phase3_learned_attributes_must_use_trailing_underscore() -> None:
    transformer = DummyPhase3Transformer()

    transformer._set_learned_attribute("selected_columns_", ["score"])

    assert hasattr(transformer, "selected_columns_")
    assert not hasattr(transformer, "selected_columns")

    with pytest.raises(ValueError, match="trailing underscore"):
        transformer._set_learned_attribute("selected_columns", ["score"])


@pytest.mark.parametrize(
    ("name", "value", "allowed"),
    [
        ("policy", "mystery", ["ignore", "raise"]),
        ("mode", "dense", ["light", "heavy"]),
    ],
)
def test_phase3_unsupported_configuration_is_rejected(
    name: str, value: str, allowed: list[str]
) -> None:
    with pytest.raises(ValueError, match=value):
        validate_supported_option(name, value, allowed=allowed)


def test_phase3_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "x", 2.5)], schema="row_id INT, label STRING, score DOUBLE"
    )

    model = DummyPhase3Model()
    model._set_learned_attribute("selected_columns_", ["score"])

    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
