"""Learned numerical imputation using Spark-native aggregations."""

from __future__ import annotations

from typing import Any, Sequence, cast

from pyspark.ml import Estimator, Model
from pyspark.ml.param import Param, Params
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from spark_feature_engine._validation import (
    to_optional_list_of_strings,
    validate_column_presence,
    validate_column_types,
    validate_unique_columns,
)


def _to_imputation_method(value: str) -> str:
    """Normalize and validate the configured imputation strategy."""
    if not isinstance(value, str):
        raise TypeError("imputation_method must be a string")

    normalized = value.strip().lower()
    if normalized not in {"mean", "median"}:
        raise ValueError("imputation_method must be either 'mean' or 'median'")
    return normalized


def _set_learned_attribute(instance: object, name: str, value: object) -> None:
    if not name.endswith("_"):
        raise ValueError("Learned attributes must use trailing underscore names")
    setattr(instance, name, value)


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


class MeanMedianImputer(
    Estimator,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Learn per-column mean or median values for numeric imputation."""

    variables = Param(
        Params._dummy(),
        "variables",
        "Selected numeric input columns. Uses discovered numeric columns when unset.",
        typeConverter=to_optional_list_of_strings,
    )
    imputation_method = Param(
        Params._dummy(),
        "imputation_method",
        "Statistic used to impute missing values.",
        typeConverter=_to_imputation_method,
    )

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        imputation_method: str = "mean",
    ) -> None:
        super().__init__()
        self._setDefault(variables=None, imputation_method="mean")
        self.set_imputation_method(imputation_method)
        if variables is not None:
            self.set_variables(variables)

    def set_variables(self, value: Sequence[str] | None) -> "MeanMedianImputer":
        """Set selected numeric input columns."""
        return cast(
            MeanMedianImputer,
            self._set(variables=to_optional_list_of_strings(value)),
        )

    def get_variables(self) -> list[str] | None:
        """Return selected input columns, if configured."""
        return cast(list[str] | None, self.getOrDefault(self.variables))

    def set_imputation_method(self, value: str) -> "MeanMedianImputer":
        """Set the learned statistic strategy."""
        return cast(MeanMedianImputer, self._set(imputation_method=value))

    def get_imputation_method(self) -> str:
        """Return the configured statistic strategy."""
        return cast(str, self.getOrDefault(self.imputation_method))

    def _fit(self, dataset: DataFrame) -> "MeanMedianImputerModel":
        variables = _resolve_numeric_variables(dataset, self.get_variables())
        imputation_method = self.get_imputation_method()

        aggregations = [
            _aggregation_for(column, imputation_method) for column in variables
        ]
        statistics_row = dataset.agg(*aggregations).collect()[0].asDict()
        imputer_dict = {
            column: float(statistics_row[column])
            for column in variables
            if statistics_row[column] is not None
        }

        return MeanMedianImputerModel(
            variables_=variables,
            imputer_dict_=imputer_dict,
            imputation_method_=imputation_method,
        )


def _aggregation_for(column: str, imputation_method: str) -> Any:
    if imputation_method == "mean":
        return F.mean(F.col(column)).alias(column)
    return F.percentile_approx(F.col(column), 0.5).alias(column)


class MeanMedianImputerModel(
    Model,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Fitted model that fills nulls with learned per-column statistics."""

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        imputer_dict_: dict[str, float],
        imputation_method_: str,
    ) -> None:
        super().__init__()
        _set_learned_attribute(self, "variables_", list(variables_))
        _set_learned_attribute(self, "imputer_dict_", dict(imputer_dict_))
        _set_learned_attribute(self, "imputation_method_", imputation_method_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="numeric")
        if not self.imputer_dict_:
            return dataset
        return dataset.fillna(self.imputer_dict_)


__all__ = ("MeanMedianImputer", "MeanMedianImputerModel")
