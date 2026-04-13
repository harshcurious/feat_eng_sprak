"""Spark-native correlated feature selection."""

from __future__ import annotations

from itertools import combinations
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_selector_threshold,
    resolve_numeric_selection_columns,
    validate_features_to_drop,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_SUPPORTED_METHODS = ("pearson",)
_MISSING_VALUE_POLICIES = ("raise", "ignore")


def _normalize_method(value: str) -> str:
    return validate_supported_option("method", value, allowed=_SUPPORTED_METHODS)


def _normalize_missing_values(value: str) -> str:
    return validate_supported_option(
        "missing_values",
        value,
        allowed=_MISSING_VALUE_POLICIES,
    )


def _correlation_edges(
    dataset: DataFrame, variables: Sequence[str], threshold: float
) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for left, right in combinations(sorted(variables), 2):
        pair_dataset = dataset.select(left, right).dropna()
        if pair_dataset.count() < 2:
            continue
        correlation = pair_dataset.stat.corr(left, right)
        if correlation is None:
            continue
        if abs(float(correlation)) >= threshold:
            edges.append((left, right))
    return edges


def _correlated_groups(
    variables: Sequence[str], edges: Sequence[tuple[str, str]]
) -> list[list[str]]:
    adjacency: dict[str, set[str]] = {variable: set() for variable in variables}
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)

    visited: set[str] = set()
    groups: list[list[str]] = []
    for variable in sorted(variables):
        if variable in visited or not adjacency[variable]:
            continue
        stack = [variable]
        component: list[str] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            stack.extend(sorted(adjacency[current] - visited, reverse=True))
        groups.append(sorted(component))
    return groups


def _features_to_drop_from_edges(edges: Sequence[tuple[str, str]]) -> list[str]:
    features_to_drop: list[str] = []
    retained: set[str] = set()
    for left, right in sorted(edges):
        retained.add(left)
        if right not in retained and right not in features_to_drop:
            features_to_drop.append(right)
    return features_to_drop


class DropCorrelatedFeatures(BaseSparkEstimator):
    """Learn and drop correlated numeric features."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        method: str = "pearson",
        threshold: float = 0.8,
        missing_values: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        self._method = _normalize_method(method)
        self._threshold = normalize_selector_threshold(threshold, name="threshold")
        self._missing_values = _normalize_missing_values(missing_values)

    def _fit(self, dataset: DataFrame) -> "DropCorrelatedFeaturesModel":
        variables = resolve_numeric_selection_columns(
            dataset, variables=self.get_variables()
        )

        if self._missing_values == "raise":
            for variable in variables:
                if dataset.where(F.col(variable).isNull()).limit(1).count():
                    raise ValueError(f"Variable '{variable}' contains missing values")

        edges = _correlation_edges(dataset, variables, self._threshold)
        correlated_feature_sets = _correlated_groups(variables, edges)
        features_to_drop = _features_to_drop_from_edges(edges)
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return DropCorrelatedFeaturesModel(
            variables_=variables,
            method_=self._method,
            threshold_=self._threshold,
            missing_values_=self._missing_values,
            correlated_feature_sets_=correlated_feature_sets,
            features_to_drop_=validated_features_to_drop,
        )


class DropCorrelatedFeaturesModel(BaseSparkModel):
    """Fitted correlated-feature selector."""

    variables_: list[str]
    method_: str
    threshold_: float
    missing_values_: str
    correlated_feature_sets_: list[list[str]]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        method_: str,
        threshold_: float,
        missing_values_: str,
        correlated_feature_sets_: Sequence[Sequence[str]],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("method_", method_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("missing_values_", missing_values_)
        self._set_learned_attribute(
            "correlated_feature_sets_",
            [list(group) for group in correlated_feature_sets_],
        )
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "method_",
            "threshold_",
            "missing_values_",
            "correlated_feature_sets_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("DropCorrelatedFeatures", "DropCorrelatedFeaturesModel")
