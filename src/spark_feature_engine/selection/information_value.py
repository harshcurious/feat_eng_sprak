"""Spark-native information value feature selection."""

from __future__ import annotations

import math
from typing import Sequence

from pyspark.sql import DataFrame

from spark_feature_engine._validation import (
    resolve_variables,
    validate_bin_count,
    validate_binary_target_column,
    validate_features_to_drop,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.drop_psi_features import _bucketed_dataset

_SUPPORTED_STRATEGIES = ("equal_width", "equal_frequency")
_EPSILON = 1e-4


def _normalize_strategy(value: str) -> str:
    return validate_supported_option("strategy", value, allowed=_SUPPORTED_STRATEGIES)


def _normalize_threshold(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("threshold must be a non-negative real number")
    converted = float(value)
    if converted < 0:
        raise ValueError("threshold must be a non-negative real number")
    return converted


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


def _compute_information_value(
    dataset: DataFrame,
    *,
    variable: str,
    target: str,
    bins: int,
    strategy: str,
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

    negative_target, positive_target = targets
    iv = 0.0
    for counts in bucket_counts.values():
        positive_rate = counts.get(positive_target, 0) / float(totals[positive_target])
        negative_rate = counts.get(negative_target, 0) / float(totals[negative_target])
        positive_rate = max(positive_rate, _EPSILON)
        negative_rate = max(negative_rate, _EPSILON)
        woe = math.log(positive_rate / negative_rate)
        iv += (positive_rate - negative_rate) * woe
    return float(iv)


class SelectByInformationValue(BaseSparkEstimator):
    """Learn and drop features below an information-value threshold."""

    def __init__(
        self,
        *,
        target: str,
        variables: Sequence[str] | None = None,
        bins: int = 5,
        strategy: str = "equal_width",
        threshold: float = 0.2,
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(target, str) or not target:
            raise TypeError("target must be a non-empty string")
        self._target = target
        try:
            self._bins = validate_bin_count(bins)
        except (TypeError, ValueError) as error:
            message = str(error).replace("bin_count", "bins")
            raise type(error)(message) from error
        self._strategy = _normalize_strategy(strategy)
        self._threshold = _normalize_threshold(threshold)

    def _fit(self, dataset: DataFrame) -> "SelectByInformationValueModel":
        target = validate_binary_target_column(dataset, target=self._target)
        variables = _resolve_selection_variables(
            dataset,
            target=target,
            variables=self.get_variables(),
        )
        information_values: dict[str, float] = {}
        features_to_drop: list[str] = []

        for variable in variables:
            iv = _compute_information_value(
                dataset.select(target, variable).where(dataset[variable].isNotNull()),
                variable=variable,
                target=target,
                bins=self._bins,
                strategy=self._strategy,
            )
            information_values[variable] = iv
            if iv < self._threshold:
                features_to_drop.append(variable)

        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return SelectByInformationValueModel(
            variables_=variables,
            target_=target,
            bins_=self._bins,
            strategy_=self._strategy,
            threshold_=self._threshold,
            information_values_=information_values,
            features_to_drop_=validated_features_to_drop,
        )


class SelectByInformationValueModel(BaseSparkModel):
    """Fitted information-value selector."""

    variables_: list[str]
    target_: str
    bins_: int
    strategy_: str
    threshold_: float
    information_values_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        bins_: int,
        strategy_: str,
        threshold_: float,
        information_values_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("bins_", bins_)
        self._set_learned_attribute("strategy_", strategy_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("information_values_", dict(information_values_))
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "bins_",
            "strategy_",
            "threshold_",
            "information_values_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("SelectByInformationValue", "SelectByInformationValueModel")
