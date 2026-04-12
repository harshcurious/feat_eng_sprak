"""Categorical imputation using Spark-native fill operations."""

from __future__ import annotations

from typing import Sequence, cast

from pyspark.ml.param import Param, Params
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from spark_feature_engine._validation import (
    to_optional_list_of_strings,
    validate_column_presence,
    validate_column_types,
    validate_unique_columns,
)
from spark_feature_engine.base import BaseSparkTransformer


def _to_fill_value(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("fill_value must be a string")
    return value


def _resolve_categorical_variables(
    dataset: DataFrame,
    variables: Sequence[str] | None,
) -> list[str]:
    normalized = to_optional_list_of_strings(variables)
    if normalized is None:
        resolved = [
            field.name
            for field in dataset.schema.fields
            if isinstance(field.dataType, StringType)
        ]
    else:
        resolved = normalized

    validate_unique_columns(resolved)
    validate_column_presence(dataset, resolved)
    validate_column_types(dataset, resolved, expected_type="string")

    if not resolved:
        raise ValueError("No string variables were found for imputation")

    return resolved


class CategoricalImputer(BaseSparkTransformer):
    """Fill selected categorical columns with a configured string value."""

    fill_value: Param[str] = Param(
        Params._dummy(),
        "fill_value",
        "Replacement string applied to missing categorical values.",
        typeConverter=_to_fill_value,
    )

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        fill_value: str = "missing",
    ) -> None:
        super().__init__(variables=variables)
        self._setDefault(fill_value="missing")
        self.set_fill_value(fill_value)

    def set_fill_value(self, value: str) -> "CategoricalImputer":
        """Set the replacement value used for missing categorical data."""
        return cast("CategoricalImputer", self._set_param(self.fill_value, value))

    def get_fill_value(self) -> str:
        """Return the configured replacement value."""
        return self.get_param_value(self.fill_value)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        variables = _resolve_categorical_variables(dataset, self.get_variables())
        fill_value = self.get_fill_value()
        if not variables:
            return dataset
        return dataset.fillna({column: fill_value for column in variables})


__all__ = ("CategoricalImputer",)
