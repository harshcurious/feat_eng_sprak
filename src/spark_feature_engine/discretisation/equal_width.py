"""Spark-native learned equal-width discretisation."""

from __future__ import annotations

import math
from typing import Sequence

from pyspark.ml.feature import Bucketizer
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_numeric_columns,
    validate_bin_count,
    validate_column_presence,
    validate_column_types,
    validate_discretisation_boundaries,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_SUPPORTED_OUTPUTS = ("bin", "boundaries")


def _normalize_output(output: str) -> str:
    return validate_supported_option("output", output, allowed=_SUPPORTED_OUTPUTS)


def _learn_boundaries(
    dataset: DataFrame,
    variables: Sequence[str],
    bin_count: int,
) -> dict[str, list[float]]:
    aggregations: list[Column] = []
    for variable in variables:
        aggregations.extend(
            (
                F.min(F.col(variable)).alias(f"{variable}__min"),
                F.max(F.col(variable)).alias(f"{variable}__max"),
            )
        )

    stats = dataset.agg(*aggregations).first()
    assert stats is not None

    boundaries: dict[str, list[float]] = {}
    for variable in variables:
        minimum = float(stats[f"{variable}__min"])
        maximum = float(stats[f"{variable}__max"])
        width = (maximum - minimum) / bin_count if maximum > minimum else 1.0
        learned = [float("-inf")]
        learned.extend(minimum + (width * index) for index in range(1, bin_count))
        learned.append(float("inf"))
        boundaries[variable] = validate_discretisation_boundaries(
            learned,
            name=f"boundaries for {variable}",
        )

    return boundaries


def _format_boundary_value(value: float) -> str:
    if math.isinf(value):
        return "-inf" if value < 0 else "inf"
    return f"{value:.1f}"


def _boundary_label(lower: float, upper: float) -> str:
    return f"({_format_boundary_value(lower)}, {_format_boundary_value(upper)}]"


def _label_expression(
    bucket_column: str,
    boundaries: Sequence[float],
    output_name: str,
) -> Column:
    bucket = F.col(bucket_column).cast("int")
    expression = F.lit(None).cast("string")
    for index, (lower, upper) in reversed(
        list(enumerate(zip(boundaries, boundaries[1:])))
    ):
        expression = F.when(
            bucket == F.lit(index),
            F.lit(_boundary_label(lower, upper)),
        ).otherwise(expression)
    return expression.alias(output_name)


def _bucket_output_expression(
    bucket_column: str,
    output_name: str,
    bin_count: int,
) -> Column:
    return (
        F.when(F.col(bucket_column) == F.lit(float(bin_count)), F.lit(None))
        .otherwise(F.col(bucket_column).cast("int"))
        .alias(output_name)
    )


class EqualWidthDiscretiser(BaseSparkEstimator):
    """Learn equal-width bin boundaries for numeric variables."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        bin_count: int = 5,
        output: str = "bin",
    ) -> None:
        super().__init__(variables=variables)
        self._bin_count = validate_bin_count(bin_count)
        self._output = _normalize_output(output)

    def _fit(self, dataset: DataFrame) -> "EqualWidthDiscretiserModel":
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        boundaries = _learn_boundaries(dataset, variables, self._bin_count)

        return EqualWidthDiscretiserModel(
            variables_=list(variables),
            bin_count_=self._bin_count,
            output_=self._output,
            boundaries_=boundaries,
        )


class EqualWidthDiscretiserModel(BaseSparkModel):
    """Fitted equal-width discretiser backed by native Spark bucketing."""

    variables_: list[str]
    bin_count_: int
    output_: str
    boundaries_: dict[str, list[float]]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        bin_count_: int,
        output_: str,
        boundaries_: dict[str, list[float]],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("bin_count_", bin_count_)
        self._set_learned_attribute("output_", output_)
        self._set_learned_attribute(
            "boundaries_",
            {
                variable: list(boundaries)
                for variable, boundaries in boundaries_.items()
            },
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "bin_count_", "output_", "boundaries_")
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="numeric")

        transformed = dataset
        temp_columns: dict[str, str] = {}
        for variable in self.variables_:
            temp_column = f"__spark_feature_engine_equal_width_{variable}_bucket"
            temp_columns[variable] = temp_column
            transformed = Bucketizer(
                splits=self.boundaries_[variable],
                inputCol=variable,
                outputCol=temp_column,
                handleInvalid="keep",
            ).transform(transformed)

        projections: list[Column] = []
        for column_name in dataset.columns:
            if column_name not in self.variables_:
                projections.append(F.col(column_name))
                continue

            if self.output_ == "bin":
                projections.append(
                    _bucket_output_expression(
                        temp_columns[column_name],
                        column_name,
                        self.bin_count_,
                    )
                )
            else:
                projections.append(
                    _label_expression(
                        temp_columns[column_name],
                        self.boundaries_[column_name],
                        column_name,
                    )
                )

        return transformed.select(*projections)


__all__ = ("EqualWidthDiscretiser", "EqualWidthDiscretiserModel")
