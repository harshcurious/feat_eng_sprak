"""Spark-native PSI-based feature selection against binary targets."""

from __future__ import annotations

import math
from typing import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from spark_feature_engine._validation import (
    normalize_selector_threshold,
    resolve_variables,
    validate_bin_count,
    validate_binary_target_column,
    validate_features_to_drop,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_SUPPORTED_STRATEGIES = ("equal_width", "equal_frequency")
_MISSING_VALUE_POLICIES = ("raise", "ignore")
_EPSILON = 1e-4


def _normalize_strategy(value: str) -> str:
    return validate_supported_option("strategy", value, allowed=_SUPPORTED_STRATEGIES)


def _normalize_missing_values(value: str) -> str:
    return validate_supported_option(
        "missing_values",
        value,
        allowed=_MISSING_VALUE_POLICIES,
    )


def _resolve_selection_variables(
    dataset: DataFrame,
    *,
    target: str,
    variables: Sequence[str] | None,
) -> list[str]:
    resolved = [
        column
        for column in resolve_variables(dataset, variables=variables)
        if column != target
    ]
    if not resolved:
        raise ValueError("variables must contain at least one non-target feature")
    return resolved


def _numeric_bucket_expression_equal_width(
    variable: str, minimum: float, maximum: float, bins: int
) -> Column:
    if minimum == maximum:
        return F.lit("0")
    width = (maximum - minimum) / float(bins)
    return (
        F.least(
            F.floor((F.col(variable) - F.lit(minimum)) / F.lit(width)),
            F.lit(bins - 1),
        )
        .cast("int")
        .cast("string")
    )


def _numeric_bucket_expression_equal_frequency(
    dataset: DataFrame,
    variable: str,
    bins: int,
) -> Column:
    quantiles = dataset.approxQuantile(
        variable,
        [step / float(bins) for step in range(1, bins)],
        0.0,
    )
    boundaries: list[float] = []
    for value in quantiles:
        if not boundaries or value > boundaries[-1]:
            boundaries.append(value)
    if not boundaries:
        return F.lit("0")

    expression = F.when(F.col(variable) <= F.lit(boundaries[0]), F.lit("0"))
    for index, boundary in enumerate(boundaries[1:], start=1):
        expression = expression.when(
            F.col(variable) <= F.lit(boundary), F.lit(str(index))
        )
    return expression.otherwise(F.lit(str(len(boundaries))))


def _bucketed_dataset(
    dataset: DataFrame,
    *,
    variable: str,
    target: str,
    bins: int,
    strategy: str,
) -> DataFrame:
    field = next(field for field in dataset.schema.fields if field.name == variable)
    if isinstance(field.dataType, NumericType):
        stats = dataset.agg(
            F.min(variable).alias("minimum"),
            F.max(variable).alias("maximum"),
        ).collect()[0]
        minimum = float(stats["minimum"])
        maximum = float(stats["maximum"])
        if strategy == "equal_width":
            bucket = _numeric_bucket_expression_equal_width(
                variable, minimum, maximum, bins
            )
        else:
            bucket = _numeric_bucket_expression_equal_frequency(dataset, variable, bins)
    else:
        bucket = F.col(variable).cast("string")

    return dataset.select(F.col(target), bucket.alias("_bucket"))


def _compute_psi(
    dataset: DataFrame, *, variable: str, target: str, bins: int, strategy: str
) -> float:
    bucketed = _bucketed_dataset(
        dataset,
        variable=variable,
        target=target,
        bins=bins,
        strategy=strategy,
    )
    grouped = bucketed.groupBy(target, "_bucket").count().collect()
    targets = [
        row[target]
        for row in bucketed.select(target).distinct().orderBy(target).collect()
    ]
    if len(targets) != 2:
        raise ValueError(
            f"Target column '{target}' must contain exactly two binary classes"
        )

    totals = {
        row[target]: int(row["count"])
        for row in bucketed.groupBy(target).count().collect()
    }
    bucket_counts: dict[str, dict[object, int]] = {}
    for row in grouped:
        bucket_counts.setdefault(str(row["_bucket"]), {})[row[target]] = int(
            row["count"]
        )

    baseline_target, comparison_target = targets
    psi_value = 0.0
    for counts in bucket_counts.values():
        baseline = counts.get(baseline_target, 0) / float(totals[baseline_target])
        comparison = counts.get(comparison_target, 0) / float(totals[comparison_target])
        baseline = max(baseline, _EPSILON)
        comparison = max(comparison, _EPSILON)
        psi_value += (comparison - baseline) * math.log(comparison / baseline)
    return float(psi_value)


class DropHighPSIFeatures(BaseSparkEstimator):
    """Learn and drop features with high target-class PSI."""

    def __init__(
        self,
        *,
        target: str,
        variables: Sequence[str] | None = None,
        threshold: float = 0.25,
        bins: int = 10,
        strategy: str = "equal_width",
        missing_values: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(target, str) or not target:
            raise TypeError("target must be a non-empty string")
        self._target = target
        self._threshold = normalize_selector_threshold(threshold, name="threshold")
        try:
            self._bins = validate_bin_count(bins)
        except (TypeError, ValueError) as error:
            message = str(error).replace("bin_count", "bins")
            raise type(error)(message) from error
        self._strategy = _normalize_strategy(strategy)
        self._missing_values = _normalize_missing_values(missing_values)

    def _fit(self, dataset: DataFrame) -> "DropHighPSIFeaturesModel":
        target = validate_binary_target_column(dataset, target=self._target)
        variables = _resolve_selection_variables(
            dataset,
            target=target,
            variables=self.get_variables(),
        )
        psi_values: dict[str, float] = {}
        features_to_drop: list[str] = []

        for variable in variables:
            variable_dataset = dataset.select(target, variable)
            if (
                self._missing_values == "raise"
                and variable_dataset.where(F.col(variable).isNull()).limit(1).count()
            ):
                raise ValueError(f"Variable '{variable}' contains missing values")
            if self._missing_values == "ignore":
                variable_dataset = variable_dataset.where(F.col(variable).isNotNull())

            psi_value = _compute_psi(
                variable_dataset,
                variable=variable,
                target=target,
                bins=self._bins,
                strategy=self._strategy,
            )
            psi_values[variable] = psi_value
            if psi_value >= self._threshold:
                features_to_drop.append(variable)

        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return DropHighPSIFeaturesModel(
            variables_=variables,
            target_=target,
            threshold_=self._threshold,
            bins_=self._bins,
            strategy_=self._strategy,
            missing_values_=self._missing_values,
            psi_values_=psi_values,
            features_to_drop_=validated_features_to_drop,
        )


class DropHighPSIFeaturesModel(BaseSparkModel):
    """Fitted PSI-based selector."""

    variables_: list[str]
    target_: str
    threshold_: float
    bins_: int
    strategy_: str
    missing_values_: str
    psi_values_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        threshold_: float,
        bins_: int,
        strategy_: str,
        missing_values_: str,
        psi_values_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("bins_", bins_)
        self._set_learned_attribute("strategy_", strategy_)
        self._set_learned_attribute("missing_values_", missing_values_)
        self._set_learned_attribute("psi_values_", dict(psi_values_))
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "threshold_",
            "bins_",
            "strategy_",
            "missing_values_",
            "psi_values_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("DropHighPSIFeatures", "DropHighPSIFeaturesModel")
