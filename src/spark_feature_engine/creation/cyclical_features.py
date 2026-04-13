"""Spark-native cyclical feature creation."""

from __future__ import annotations

import math
from numbers import Real
from typing import Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_max_values,
    resolve_numeric_columns,
    validate_column_presence,
    validate_column_types,
    validate_generated_column_names,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel


def _normalize_drop_original(value: bool) -> bool:
    if not isinstance(value, bool):
        raise TypeError("drop_original must be a boolean")
    return value


def _normalize_configured_max_values(
    value: Mapping[str, Real] | None,
) -> dict[str, Real] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError(
            "max_values must be a mapping of variable names to numeric values"
        )
    if any(not isinstance(key, str) for key in value):
        raise TypeError("max_values keys must be string variable names")
    return {str(key): value[key] for key in value}


def _learn_max_values(dataset: DataFrame, variables: Sequence[str]) -> dict[str, float]:
    aggregations = [F.max(F.col(variable)).alias(variable) for variable in variables]
    maxima = dataset.agg(*aggregations).first()
    assert maxima is not None
    return normalize_max_values(
        {variable: maxima[variable] for variable in variables},
        variables=variables,
    )


def _cyclical_expressions(variable: str, max_value: float) -> tuple[Column, Column]:
    scale = F.lit((2.0 * math.pi) / max_value)
    variable_column = F.col(variable)
    return (
        F.sin(variable_column * scale).alias(f"{variable}_sin"),
        F.cos(variable_column * scale).alias(f"{variable}_cos"),
    )


class CyclicalFeatures(BaseSparkEstimator):
    """Learn or apply cyclical maxima for numeric variables."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        max_values: Mapping[str, Real] | None = None,
        drop_original: bool = False,
    ) -> None:
        super().__init__(variables=variables)
        self._configured_max_values = _normalize_configured_max_values(max_values)
        self._drop_original = _normalize_drop_original(drop_original)

    def _fit(self, dataset: DataFrame) -> "CyclicalFeaturesModel":
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        max_values = (
            normalize_max_values(self._configured_max_values, variables=variables)
            if self._configured_max_values is not None
            else _learn_max_values(dataset, variables)
        )

        return CyclicalFeaturesModel(
            variables_=variables,
            max_values_=max_values,
            drop_original_=self._drop_original,
        )


class CyclicalFeaturesModel(BaseSparkModel):
    """Fitted cyclical feature creator backed by native Spark expressions."""

    variables_: list[str]
    max_values_: dict[str, float]
    drop_original_: bool

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        max_values_: Mapping[str, float],
        drop_original_: bool,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("max_values_", dict(max_values_))
        self._set_learned_attribute("drop_original_", drop_original_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "max_values_", "drop_original_")
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="numeric")

        generated_columns = [
            generated
            for variable in self.variables_
            for generated in (f"{variable}_sin", f"{variable}_cos")
        ]
        validate_generated_column_names(
            dataset,
            generated_columns,
            ignore_existing=self.variables_ if self.drop_original_ else (),
        )

        projections: list[Column] = []
        for column_name in dataset.columns:
            if self.drop_original_ and column_name in self.variables_:
                continue
            projections.append(F.col(column_name))

        for variable in self.variables_:
            projections.extend(
                _cyclical_expressions(variable, self.max_values_[variable])
            )

        return dataset.select(*projections)


__all__ = ("CyclicalFeatures", "CyclicalFeaturesModel")
