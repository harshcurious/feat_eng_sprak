"""Spark-native smart correlated feature selection."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_selection_method,
    normalize_selector_threshold,
    resolve_numeric_selection_columns,
    validate_features_to_drop,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.drop_correlated_features import (
    _correlated_groups,
    _correlation_edges,
)

_SUPPORTED_SELECTION_METHODS = ("missing_values", "cardinality", "variance")


def _selection_score(
    dataset: DataFrame, variable: str, selection_method: str
) -> tuple[float, str]:
    if selection_method == "missing_values":
        missing_count = dataset.where(F.col(variable).isNull()).count()
        return (float(-missing_count), variable)
    if selection_method == "cardinality":
        cardinality = dataset.select(variable).distinct().count()
        return (float(cardinality), variable)

    variance_row = dataset.select(
        F.variance(F.col(variable)).alias("variance")
    ).collect()[0]
    variance = variance_row["variance"]
    return (float(variance or 0.0), variable)


class SmartCorrelatedSelection(BaseSparkEstimator):
    """Learn a representative feature from each correlated group."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        threshold: float = 0.8,
        selection_method: str = "missing_values",
    ) -> None:
        super().__init__(variables=variables)
        self._threshold = normalize_selector_threshold(threshold, name="threshold")
        self._selection_method = normalize_selection_method(
            selection_method,
            allowed=_SUPPORTED_SELECTION_METHODS,
        )

    def _fit(self, dataset: DataFrame) -> "SmartCorrelatedSelectionModel":
        variables = resolve_numeric_selection_columns(
            dataset, variables=self.get_variables()
        )
        edges = _correlation_edges(dataset, variables, self._threshold)
        correlated_feature_sets = _correlated_groups(variables, edges)

        selected_features: list[str] = []
        features_to_drop: list[str] = []
        for group in correlated_feature_sets:
            best_feature = max(
                group,
                key=lambda variable: _selection_score(
                    dataset, variable, self._selection_method
                ),
            )
            selected_features.append(best_feature)
            features_to_drop.extend(
                [variable for variable in group if variable != best_feature]
            )

        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return SmartCorrelatedSelectionModel(
            variables_=variables,
            threshold_=self._threshold,
            selection_method_=self._selection_method,
            correlated_feature_sets_=correlated_feature_sets,
            selected_features_=selected_features,
            features_to_drop_=validated_features_to_drop,
        )


class SmartCorrelatedSelectionModel(BaseSparkModel):
    """Fitted smart correlated selector."""

    variables_: list[str]
    threshold_: float
    selection_method_: str
    correlated_feature_sets_: list[list[str]]
    selected_features_: list[str]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        threshold_: float,
        selection_method_: str,
        correlated_feature_sets_: Sequence[Sequence[str]],
        selected_features_: Sequence[str],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("selection_method_", selection_method_)
        self._set_learned_attribute(
            "correlated_feature_sets_",
            [list(group) for group in correlated_feature_sets_],
        )
        self._set_learned_attribute("selected_features_", list(selected_features_))
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "threshold_",
            "selection_method_",
            "correlated_feature_sets_",
            "selected_features_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("SmartCorrelatedSelection", "SmartCorrelatedSelectionModel")
