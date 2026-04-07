"""Shared PySpark ML transformer foundation."""

from __future__ import annotations

from typing import Any, Sequence, TypeVar, cast

from pyspark.ml import Estimator, Model, Transformer
from pyspark.ml.param import Param, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame

from ._validation import (
    ColumnExpectation,
    resolve_variables,
    to_optional_list_of_strings,
    validate_column_types,
    validate_fitted_attributes,
    validate_learned_attribute_name,
)

_TransformerT = TypeVar("_TransformerT", bound="BaseSparkTransformer")
_EstimatorT = TypeVar("_EstimatorT", bound="BaseSparkEstimator")
_ValueT = TypeVar("_ValueT")


class _BaseSparkFeaturesMixin:
    """Shared parameter and learned-state helpers for Spark ML components."""

    def _set_param(self, param: Param[Any], value: Any) -> Any:
        return self._set(**{param.name: value})

    def get_param_value(self, param: Param[_ValueT]) -> _ValueT:
        return cast(_ValueT, self.getOrDefault(param))

    def get_required_param(self, param: Param[_ValueT]) -> _ValueT:
        if not self.isSet(param) and not self.hasDefault(param):
            raise ValueError(f"Parameter '{param.name}' must be set before use")
        return self.get_param_value(param)

    def validate_column_types(
        self,
        dataset: DataFrame,
        columns: Sequence[str],
        *,
        expected_type: ColumnExpectation = "any",
    ) -> None:
        validate_column_types(dataset, columns, expected_type=expected_type)

    def _set_learned_attribute(self, name: str, value: Any) -> None:
        setattr(self, validate_learned_attribute_name(name), value)

    def require_fitted(self, *attribute_names: str) -> None:
        validate_fitted_attributes(self, attribute_names)


class _VariablesParamMixin(_BaseSparkFeaturesMixin):
    """Shared variable-parameter behavior for estimators and transformers."""

    variables = Param(
        Params._dummy(),
        "variables",
        "Selected input columns. Uses all dataset columns when unset.",
        typeConverter=to_optional_list_of_strings,
    )

    def _init_variables_param(self, *, variables: Sequence[str] | None = None) -> None:
        self._setDefault(variables=None)
        if variables is not None:
            self.set_variables(variables)

    def set_variables(self, value: Sequence[str] | None) -> Any:
        """Set selected input columns."""
        return self._set_param(self.variables, value)

    def get_variables(self) -> list[str] | None:
        """Return selected input columns, if configured."""
        return self.get_param_value(self.variables)

    def resolve_variables(
        self,
        dataset: DataFrame,
        *,
        expected_type: ColumnExpectation = "any",
    ) -> list[str]:
        """Resolve configured target columns against the dataset schema."""
        return resolve_variables(
            dataset,
            variables=self.get_variables(),
            expected_type=expected_type,
        )


class BaseSparkTransformer(
    _VariablesParamMixin,
    Transformer,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Common Spark ML transformer behavior for the library."""

    def __init__(self, *, variables: Sequence[str] | None = None) -> None:
        super().__init__()
        self._init_variables_param(variables=variables)


class BaseSparkEstimator(
    _VariablesParamMixin,
    Estimator,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Common Spark ML estimator behavior for learned Phase 2 components."""

    def __init__(self, *, variables: Sequence[str] | None = None) -> None:
        super().__init__()
        self._init_variables_param(variables=variables)

    def set_variables(self: _EstimatorT, value: Sequence[str] | None) -> _EstimatorT:
        return cast(_EstimatorT, super().set_variables(value))


class BaseSparkModel(
    _BaseSparkFeaturesMixin,
    Model,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Common fitted-model behavior for learned Phase 2 components."""


__all__ = ("BaseSparkEstimator", "BaseSparkModel", "BaseSparkTransformer")
