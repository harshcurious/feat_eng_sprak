"""Spark-native row-wise math feature creation."""

from __future__ import annotations

from functools import reduce
from typing import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_creation_functions,
    resolve_numeric_columns,
    to_optional_list_of_strings,
    validate_generated_column_names,
    validate_minimum_variable_count,
    validate_unique_columns,
)
from spark_feature_engine.base import BaseSparkTransformer

_SUPPORTED_FUNCTIONS = ("sum", "mean", "min", "max", "prod")


def _normalize_drop_original(value: bool) -> bool:
    if not isinstance(value, bool):
        raise TypeError("drop_original must be a boolean")
    return value


def _normalize_new_variable_names(
    value: Sequence[str] | None,
) -> list[str] | None:
    normalized = to_optional_list_of_strings(value)
    if normalized is None:
        return None
    validate_unique_columns(normalized)
    return normalized


def _default_output_names(
    variables: Sequence[str], functions: Sequence[str]
) -> list[str]:
    joined_variables = "_".join(variables)
    return [f"{function}_{joined_variables}" for function in functions]


def _resolve_output_names(
    *,
    variables: Sequence[str],
    functions: Sequence[str],
    new_variable_names: Sequence[str] | None,
) -> list[str]:
    if new_variable_names is None:
        return _default_output_names(variables, functions)
    if len(new_variable_names) != len(functions):
        raise ValueError(
            "The number of new feature names must coincide with the number of functions."
        )
    return list(new_variable_names)


def _sum_expression(columns: Sequence[Column]) -> Column:
    return reduce(lambda left, right: left + right, columns[1:], columns[0])


def _prod_expression(columns: Sequence[Column]) -> Column:
    return reduce(lambda left, right: left * right, columns[1:], columns[0])


def _math_expression(
    *,
    function_name: str,
    variables: Sequence[str],
    output_name: str,
) -> Column:
    input_columns = [F.col(variable) for variable in variables]
    if function_name == "sum":
        expression = _sum_expression(input_columns)
    elif function_name == "mean":
        expression = _sum_expression(input_columns) / F.lit(float(len(input_columns)))
    elif function_name == "min":
        expression = F.least(*input_columns)
    elif function_name == "max":
        expression = F.greatest(*input_columns)
    else:
        expression = _prod_expression(input_columns)
    return expression.alias(output_name)


class MathFeatures(BaseSparkTransformer):
    """Create row-wise aggregate features from selected numeric variables."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        func: Sequence[str],
        new_variable_names: Sequence[str] | None = None,
        drop_original: bool = False,
    ) -> None:
        super().__init__(variables=variables)
        self._functions = normalize_creation_functions(
            func, allowed=_SUPPORTED_FUNCTIONS
        )
        self._new_variable_names = _normalize_new_variable_names(new_variable_names)
        self._drop_original = _normalize_drop_original(drop_original)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        validate_minimum_variable_count(variables, minimum=2, name="variables")

        output_names = _resolve_output_names(
            variables=variables,
            functions=self._functions,
            new_variable_names=self._new_variable_names,
        )
        validate_generated_column_names(
            dataset,
            output_names,
            ignore_existing=variables if self._drop_original else (),
        )

        projections: list[Column] = []
        for column_name in dataset.columns:
            if self._drop_original and column_name in variables:
                continue
            projections.append(F.col(column_name))

        for function_name, output_name in zip(self._functions, output_names):
            projections.append(
                _math_expression(
                    function_name=function_name,
                    variables=variables,
                    output_name=output_name,
                )
            )

        return dataset.select(*projections)


__all__ = ("MathFeatures",)
