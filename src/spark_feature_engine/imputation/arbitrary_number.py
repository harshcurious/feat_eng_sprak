"""Stateless numerical imputation using Spark-native fill behavior."""

from __future__ import annotations

from typing import Sequence, cast

from pyspark.ml.param import Param, Params
from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType

from spark_feature_engine._validation import (
    to_optional_list_of_strings,
    validate_column_presence,
    validate_column_types,
    validate_unique_columns,
)
from spark_feature_engine.base import BaseSparkTransformer


def _resolve_numeric_variables(
    dataset: DataFrame,
    variables: Sequence[str] | None,
) -> list[str]:
    normalized = to_optional_list_of_strings(variables)
    if normalized is None:
        resolved = [
            field.name
            for field in dataset.schema.fields
            if isinstance(field.dataType, NumericType)
        ]
    else:
        resolved = normalized

    validate_unique_columns(resolved)
    validate_column_presence(dataset, resolved)
    validate_column_types(dataset, resolved, expected_type="numeric")

    if not resolved:
        raise ValueError("No numeric variables were found for imputation")

    return resolved


class ArbitraryNumberImputer(BaseSparkTransformer):
    """Fill selected numeric columns with a constant value."""

    fill_value: Param[float] = Param(
        Params._dummy(),
        "fill_value",
        "Constant value used to fill missing numeric entries.",
        typeConverter=float,
    )

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        fill_value: float = 0.0,
    ) -> None:
        super().__init__(variables=variables)
        self._setDefault(fill_value=0.0)
        self.set_fill_value(fill_value)

    def set_fill_value(self, value: float) -> "ArbitraryNumberImputer":
        """Set the constant used to replace missing values."""
        return cast(ArbitraryNumberImputer, self._set_param(self.fill_value, value))

    def get_fill_value(self) -> float:
        """Return the configured fill value."""
        return self.get_param_value(self.fill_value)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        variables = _resolve_numeric_variables(dataset, self.get_variables())
        return dataset.na.fill(self.get_fill_value(), subset=variables)


__all__ = ("ArbitraryNumberImputer",)
