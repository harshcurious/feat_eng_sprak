"""Spark-native learned equal-frequency discretisation."""

from __future__ import annotations

from typing import Sequence

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
from spark_feature_engine.discretisation.equal_width import (
    _boundary_label,
)

_SUPPORTED_OUTPUTS = ("bin", "boundaries")
_RELATIVE_ERROR = 0.0


def _normalize_output(output: str) -> str:
    return validate_supported_option("output", output, allowed=_SUPPORTED_OUTPUTS)


def _bucket_output_expression(
    variable: str,
    boundaries: Sequence[float],
    output_name: str,
) -> Column:
    value = F.col(variable)
    expression = F.lit(None).cast("int")
    for index, upper in reversed(list(enumerate(boundaries[1:]))):
        expression = F.when(value <= F.lit(upper), F.lit(index)).otherwise(expression)
    return expression.alias(output_name)


def _label_expression(
    variable: str,
    boundaries: Sequence[float],
    output_name: str,
) -> Column:
    value = F.col(variable)
    expression = F.lit(None).cast("string")
    for index, upper in reversed(list(enumerate(boundaries[1:]))):
        expression = F.when(
            value <= F.lit(upper),
            F.lit(_boundary_label(boundaries[index], upper)),
        ).otherwise(expression)
    return expression.alias(output_name)


def _learn_boundaries(
    dataset: DataFrame,
    variables: Sequence[str],
    bin_count: int,
) -> dict[str, list[float]]:
    probabilities = [index / bin_count for index in range(1, bin_count)]
    quantiles = dataset.approxQuantile(list(variables), probabilities, _RELATIVE_ERROR)
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
    for variable, variable_quantiles in zip(variables, quantiles):
        minimum = float(stats[f"{variable}__min"])
        maximum = float(stats[f"{variable}__max"])
        internal_boundaries: list[float] = []
        for quantile in variable_quantiles:
            quantile_value = float(quantile)
            if quantile_value in (maximum, float("inf"), float("-inf")):
                continue
            if internal_boundaries and quantile_value <= internal_boundaries[-1]:
                continue
            internal_boundaries.append(quantile_value)

        if maximum > minimum and len(internal_boundaries) < (bin_count - 1):
            width = (maximum - minimum) / bin_count
            for index in range(1, bin_count):
                candidate = minimum + (width * index)
                if candidate >= maximum:
                    continue
                if candidate in internal_boundaries:
                    continue
                internal_boundaries.append(candidate)
                internal_boundaries.sort()
                if len(internal_boundaries) == (bin_count - 1):
                    break

        learned = [float("-inf"), *internal_boundaries[: bin_count - 1], float("inf")]
        boundaries[variable] = validate_discretisation_boundaries(
            learned,
            name=f"boundaries for {variable}",
        )

    return boundaries


class EqualFrequencyDiscretiser(BaseSparkEstimator):
    """Learn equal-frequency bin boundaries for numeric variables."""

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

    def _fit(self, dataset: DataFrame) -> "EqualFrequencyDiscretiserModel":
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        boundaries = _learn_boundaries(dataset, variables, self._bin_count)

        return EqualFrequencyDiscretiserModel(
            variables_=list(variables),
            bin_count_=self._bin_count,
            output_=self._output,
            boundaries_=boundaries,
        )


class EqualFrequencyDiscretiserModel(BaseSparkModel):
    """Fitted equal-frequency discretiser backed by native Spark bucketing."""

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

        projections: list[Column] = []
        for column_name in dataset.columns:
            if column_name not in self.variables_:
                projections.append(F.col(column_name))
                continue

            if self.output_ == "bin":
                projections.append(
                    _bucket_output_expression(
                        column_name,
                        self.boundaries_[column_name],
                        column_name,
                    )
                )
            else:
                projections.append(
                    _label_expression(
                        column_name,
                        self.boundaries_[column_name],
                        column_name,
                    )
                )

        return dataset.select(*projections)


__all__ = ("EqualFrequencyDiscretiser", "EqualFrequencyDiscretiserModel")
