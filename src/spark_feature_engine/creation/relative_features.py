"""Spark-native relative feature creation."""

from __future__ import annotations

from numbers import Real
from typing import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_creation_functions,
    resolve_numeric_columns,
    resolve_relative_feature_variables,
    to_optional_list_of_strings,
    validate_generated_column_names,
)
from spark_feature_engine.base import BaseSparkTransformer

_SUPPORTED_FUNCTIONS = (
    "add",
    "sub",
    "mul",
    "div",
    "truediv",
    "floordiv",
    "mod",
    "pow",
)
_DIVISION_FUNCTIONS = ("div", "truediv", "floordiv", "mod")


def _normalize_reference(value: Sequence[str]) -> list[str]:
    normalized = to_optional_list_of_strings(value)
    if normalized is None:
        raise ValueError("reference must contain at least one column name")
    if not normalized:
        raise ValueError("reference must contain at least one column name")
    return normalized


def _normalize_fill_value(value: float | None) -> float | None:
    if value is None:
        return None
    if not isinstance(value, Real) or isinstance(value, bool):
        raise TypeError("fill_value must be a real number or None")
    return float(value)


def _normalize_drop_original(value: bool) -> bool:
    if not isinstance(value, bool):
        raise TypeError("drop_original must be a boolean")
    return value


def _generated_name(variable: str, function_name: str, reference: str) -> str:
    return f"{variable}_{function_name}_{reference}"


def _division_guard_expression(
    *,
    reference: str,
    expression: Column,
    fill_value: float | None,
) -> Column:
    if fill_value is None:
        return expression
    return F.when(F.col(reference) == F.lit(0), F.lit(fill_value)).otherwise(expression)


def _relative_expression(
    *,
    variable: str,
    function_name: str,
    reference: str,
    fill_value: float | None,
) -> Column:
    variable_column = F.col(variable)
    reference_column = F.col(reference)
    if function_name == "sub":
        expression = variable_column - reference_column
    elif function_name == "add":
        expression = variable_column + reference_column
    elif function_name == "mul":
        expression = variable_column * reference_column
    elif function_name in ("div", "truediv"):
        expression = _division_guard_expression(
            reference=reference,
            expression=variable_column / reference_column,
            fill_value=fill_value,
        )
    elif function_name == "floordiv":
        expression = _division_guard_expression(
            reference=reference,
            expression=F.floor(variable_column / reference_column),
            fill_value=fill_value,
        )
    elif function_name == "mod":
        expression = _division_guard_expression(
            reference=reference,
            expression=variable_column % reference_column,
            fill_value=fill_value,
        )
    else:
        expression = F.pow(variable_column, reference_column)
    return expression.alias(_generated_name(variable, function_name, reference))


class RelativeFeatures(BaseSparkTransformer):
    """Create arithmetic features between selected and reference columns."""

    def __init__(
        self,
        *,
        reference: Sequence[str],
        func: Sequence[str],
        variables: Sequence[str] | None = None,
        fill_value: float | None = None,
        drop_original: bool = False,
    ) -> None:
        super().__init__(variables=variables)
        self._reference = _normalize_reference(reference)
        self._functions = normalize_creation_functions(
            func, allowed=_SUPPORTED_FUNCTIONS
        )
        self._fill_value = _normalize_fill_value(fill_value)
        self._drop_original = _normalize_drop_original(drop_original)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        variables = resolve_relative_feature_variables(
            dataset,
            variables=self.get_variables(),
            reference=self._reference,
        )
        reference = resolve_numeric_columns(dataset, variables=self._reference)

        if self._fill_value is None and any(
            function_name in _DIVISION_FUNCTIONS for function_name in self._functions
        ):
            zero_flags = dataset.agg(
                *[
                    F.max(
                        F.when(F.col(reference_name) == F.lit(0), F.lit(1)).otherwise(
                            F.lit(0)
                        )
                    ).alias(reference_name)
                    for reference_name in reference
                ]
            ).first()
            assert zero_flags is not None
            zero_references = [
                reference_name
                for reference_name in reference
                if zero_flags[reference_name] == 1
            ]
            if zero_references:
                joined = ", ".join(zero_references)
                raise ValueError(
                    "Division by zero does not exist. "
                    f"Zero values found in reference column(s): {joined}"
                )

        output_names = [
            _generated_name(variable, function_name, reference_name)
            for function_name in self._functions
            for reference_name in reference
            for variable in variables
        ]
        validate_generated_column_names(
            dataset,
            output_names,
            ignore_existing=(*variables, *reference) if self._drop_original else (),
        )

        projections: list[Column] = []
        dropped = set((*variables, *reference)) if self._drop_original else set()
        for column_name in dataset.columns:
            if column_name in dropped:
                continue
            projections.append(F.col(column_name))

        for function_name in self._functions:
            for reference_name in reference:
                for variable in variables:
                    projections.append(
                        _relative_expression(
                            variable=variable,
                            function_name=function_name,
                            reference=reference_name,
                            fill_value=self._fill_value,
                        )
                    )

        return dataset.select(*projections)


__all__ = ("RelativeFeatures",)
