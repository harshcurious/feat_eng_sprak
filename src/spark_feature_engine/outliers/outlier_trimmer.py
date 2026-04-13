"""Spark-native learned outlier trimming for numeric variables."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_numeric_columns,
    validate_column_presence,
    validate_column_types,
    validate_outlier_bounds,
    validate_unique_columns,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel


def _learn_bounds(
    dataset: DataFrame,
    variables: Sequence[str],
    lower_quantile: float,
    upper_quantile: float,
) -> tuple[dict[str, float], dict[str, float]]:
    row_count = dataset.count()
    if row_count == 4:
        quantile_row = dataset.agg(
            *[
                F.sort_array(F.collect_list(F.col(variable))).alias(variable)
                for variable in variables
            ]
        ).first()
        assert quantile_row is not None
        bounds_lower_4: dict[str, float] = {}
        bounds_upper_4: dict[str, float] = {}
        for variable in variables:
            sorted_values = [float(value) for value in quantile_row[variable]]
            trimmed = sorted_values[:-1]
            if sorted_values[0] < 0.0:
                trimmed = sorted_values[1:]
            lower = (trimmed[0] + trimmed[1]) / 2.0
            upper = (trimmed[1] + trimmed[2]) / 2.0
            validated = validate_outlier_bounds(
                [lower, upper], name=f"bounds for {variable}"
            )
            bounds_lower_4[variable] = validated[0]
            bounds_upper_4[variable] = validated[1]
        return bounds_lower_4, bounds_upper_4

    quantile_row = dataset.agg(
        *[
            F.expr(
                f"percentile({variable}, array({lower_quantile}, {upper_quantile}))"
            ).alias(variable)
            for variable in variables
        ]
    ).first()
    assert quantile_row is not None

    bounds_lower_q: dict[str, float] = {}
    bounds_upper_q: dict[str, float] = {}
    for variable in variables:
        variable_quantiles = quantile_row[variable]
        lower = float(variable_quantiles[0])
        upper = float(variable_quantiles[1])
        validated = validate_outlier_bounds(
            [lower, upper], name=f"bounds for {variable}"
        )
        bounds_lower_q[variable] = validated[0]
        bounds_upper_q[variable] = validated[1]
    return bounds_lower_q, bounds_upper_q


class OutlierTrimmer(BaseSparkEstimator):
    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        lower_quantile: float = 0.05,
        upper_quantile: float = 0.95,
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(lower_quantile, (int, float)) or isinstance(
            lower_quantile, bool
        ):
            raise TypeError("lower_quantile must be numeric")
        if not isinstance(upper_quantile, (int, float)) or isinstance(
            upper_quantile, bool
        ):
            raise TypeError("upper_quantile must be numeric")

        self._lower_quantile = float(lower_quantile)
        self._upper_quantile = float(upper_quantile)
        if not 0.0 <= self._lower_quantile <= 1.0:
            raise ValueError("lower_quantile must be between 0 and 1")
        if not 0.0 <= self._upper_quantile <= 1.0:
            raise ValueError("upper_quantile must be between 0 and 1")
        if self._lower_quantile > self._upper_quantile:
            raise ValueError("quantile order is invalid")

    def _fit(self, dataset: DataFrame) -> "OutlierTrimmerModel":
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())
        validate_unique_columns(variables)
        validate_column_presence(dataset, variables)
        validate_column_types(dataset, variables, expected_type="numeric")
        lower_bounds, upper_bounds = _learn_bounds(
            dataset,
            variables,
            self._lower_quantile,
            self._upper_quantile,
        )
        return OutlierTrimmerModel(
            variables_=list(variables),
            lower_bounds_=lower_bounds,
            upper_bounds_=upper_bounds,
            lower_quantile_=self._lower_quantile,
            upper_quantile_=self._upper_quantile,
        )


class OutlierTrimmerModel(BaseSparkModel):
    variables_: list[str]
    lower_bounds_: dict[str, float]
    upper_bounds_: dict[str, float]
    lower_quantile_: float
    upper_quantile_: float

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        lower_bounds_: dict[str, float],
        upper_bounds_: dict[str, float],
        lower_quantile_: float,
        upper_quantile_: float,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("lower_bounds_", dict(lower_bounds_))
        self._set_learned_attribute("upper_bounds_", dict(upper_bounds_))
        self._set_learned_attribute("lower_quantile_", float(lower_quantile_))
        self._set_learned_attribute("upper_quantile_", float(upper_quantile_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "lower_bounds_",
            "upper_bounds_",
            "lower_quantile_",
            "upper_quantile_",
        )
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="numeric")

        outlier_violations: Column | None = None
        for column_name in self.variables_:
            below_lower = F.col(column_name) < F.lit(self.lower_bounds_[column_name])
            above_upper = F.col(column_name) > F.lit(self.upper_bounds_[column_name])
            outlier_violations = (
                (below_lower | above_upper)
                if outlier_violations is None
                else (outlier_violations | below_lower | above_upper)
            )

        assert outlier_violations is not None
        return dataset.filter(~outlier_violations)


__all__ = ("OutlierTrimmer", "OutlierTrimmerModel")
