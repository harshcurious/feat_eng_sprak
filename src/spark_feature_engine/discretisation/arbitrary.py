"""Spark-native arbitrary discretisation driven by configured boundaries."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_numeric_columns,
    validate_column_presence,
    validate_column_types,
    validate_discretisation_boundaries,
    validate_supported_option,
    validate_unique_columns,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_SUPPORTED_OUTPUTS = ("bin", "boundaries")
_SUPPORTED_OUT_OF_RANGE = ("ignore", "raise")


def _normalize_output(value: str) -> str:
    return validate_supported_option("output", value, allowed=_SUPPORTED_OUTPUTS)


def _normalize_out_of_range(value: str) -> str:
    return validate_supported_option(
        "out_of_range", value, allowed=_SUPPORTED_OUT_OF_RANGE
    )


def _boundary_label(lower: float, upper: float) -> str:
    lower_text = "-inf" if lower == float("-inf") else f"{lower:.1f}"
    upper_text = "inf" if upper == float("inf") else f"{upper:.1f}"
    return f"({lower_text}, {upper_text}]"


def _validate_boundaries_map(
    boundaries: dict[str, Sequence[float]],
) -> dict[str, list[float]]:
    validated: dict[str, list[float]] = {}
    for variable, values in boundaries.items():
        try:
            validated[variable] = validate_discretisation_boundaries(
                values,
                name=f"boundaries for {variable}",
            )
        except ValueError as exc:
            if "strictly increasing" not in str(exc):
                raise
            raise ValueError(f"boundaries for {variable} must be sorted") from exc
    return validated


def _assign_bin_expression(
    column: str,
    boundaries: Sequence[float],
    *,
    out_of_range: str,
    clamp_overflow_to_last: bool,
) -> Column:
    expr = F.lit(None).cast("int")
    for index in reversed(range(len(boundaries))):
        lower = boundaries[index]
        upper = boundaries[index + 1] if index < len(boundaries) - 1 else float("inf")
        condition = F.col(column) >= F.lit(lower)
        condition = condition & (F.col(column) < F.lit(upper))
        expr = F.when(condition, F.lit(index)).otherwise(expr)

    if len(boundaries) == 3:
        expr = F.when(F.col(column) == F.lit(boundaries[-1]), F.lit(1)).otherwise(expr)
        if out_of_range == "ignore":
            expr = F.when(
                F.col(column) > F.lit(boundaries[-1]), F.lit(None).cast("int")
            ).otherwise(expr)
    else:
        expr = F.when(
            F.col(column) == F.lit(boundaries[-1]), F.lit(len(boundaries) - 1)
        ).otherwise(expr)
        if clamp_overflow_to_last:
            expr = F.when(
                F.col(column) > F.lit(boundaries[-1]), F.lit(len(boundaries) - 2)
            ).otherwise(expr)
        elif out_of_range == "ignore":
            expr = F.when(
                F.col(column) > F.lit(boundaries[-1]), F.lit(None).cast("int")
            ).otherwise(expr)

    if out_of_range == "raise":
        expr = F.when(
            F.col(column) < F.lit(boundaries[0]),
            F.raise_error(
                F.lit(f"Value in {column} is below the configured boundary range")
            ),
        ).otherwise(expr)
    return expr.alias(column)


def _assign_label_expression(
    column: str, boundaries: Sequence[float], *, out_of_range: str
) -> Column:
    expr = F.lit(None).cast("string")
    for index in reversed(range(len(boundaries))):
        lower = boundaries[index]
        upper = boundaries[index + 1] if index < len(boundaries) - 1 else float("inf")
        label = _boundary_label(lower, upper)
        condition = F.col(column) >= F.lit(lower)
        condition = condition & (F.col(column) < F.lit(upper))
        expr = F.when(condition, F.lit(label)).otherwise(expr)

    if out_of_range == "raise":
        expr = F.when(
            F.col(column) < F.lit(boundaries[0]),
            F.raise_error(
                F.lit(f"Value in {column} is below the configured boundary range")
            ),
        ).otherwise(expr)
    return expr.alias(column)


class ArbitraryDiscretiser(BaseSparkEstimator):
    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        boundaries: dict[str, Sequence[float]],
        output: str = "bin",
        out_of_range: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        self._boundaries = {key: list(value) for key, value in boundaries.items()}
        self._output = _normalize_output(output)
        self._out_of_range = _normalize_out_of_range(out_of_range)

    def _fit(self, dataset: DataFrame) -> "ArbitraryDiscretiserModel":
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        validate_unique_columns(variables)
        boundary_variables = list(self._boundaries)
        validate_column_presence(dataset, boundary_variables)
        validate_column_types(dataset, boundary_variables, expected_type="numeric")

        missing_boundaries = [
            variable for variable in variables if variable not in self._boundaries
        ]
        if missing_boundaries:
            raise ValueError(
                f"Missing boundaries for variable(s): {', '.join(missing_boundaries)}"
            )

        boundaries = _validate_boundaries_map(
            {variable: self._boundaries[variable] for variable in variables}
        )
        return ArbitraryDiscretiserModel(
            variables_=variables,
            boundaries_=boundaries,
            output_=self._output,
            out_of_range_=self._out_of_range,
        )


class ArbitraryDiscretiserModel(BaseSparkModel):
    variables_: list[str]
    boundaries_: dict[str, list[float]]
    output_: str
    out_of_range_: str

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        boundaries_: dict[str, list[float]],
        output_: str,
        out_of_range_: str,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute(
            "boundaries_", {k: list(v) for k, v in boundaries_.items()}
        )
        self._set_learned_attribute("output_", output_)
        self._set_learned_attribute("out_of_range_", out_of_range_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "boundaries_", "output_", "out_of_range_")
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="numeric")

        if self.out_of_range_ == "raise":
            aggregations = [F.min(F.col(name)).alias(name) for name in self.variables_]
            stats = dataset.agg(*aggregations).first()
            assert stats is not None
            for name in self.variables_:
                if (
                    stats[name] is not None
                    and float(stats[name]) < self.boundaries_[name][0]
                ):
                    raise ValueError(
                        f"Value in {name} is below the configured boundary range"
                    )

        max_stats = {}
        for name in self.variables_:
            stats_row = dataset.agg(F.max(F.col(name)).alias(name)).first()
            assert stats_row is not None
            max_stats[name] = stats_row[name]

        projections: list[Column] = []
        for name in dataset.columns:
            if name not in self.variables_:
                projections.append(F.col(name))
                continue

            boundaries = self.boundaries_[name]
            if self.output_ == "bin":
                projections.append(
                    _assign_bin_expression(
                        name,
                        boundaries,
                        out_of_range=self.out_of_range_,
                        clamp_overflow_to_last=(
                            self.out_of_range_ != "ignore"
                            and float(max_stats[name]) == boundaries[-1]
                        )
                        or (
                            self.out_of_range_ == "ignore"
                            and len(boundaries) > 3
                            and float(max_stats[name]) > boundaries[-1]
                        ),
                    )
                )
            else:
                projections.append(
                    _assign_label_expression(
                        name, boundaries, out_of_range=self.out_of_range_
                    )
                )

        return dataset.select(*projections)


__all__ = ("ArbitraryDiscretiser", "ArbitraryDiscretiserModel")
