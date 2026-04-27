"""Spark-native recursive feature elimination."""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.sql import DataFrame

from spark_feature_engine._validation import (
    normalize_classification_scoring,
    normalize_selector_threshold,
    validate_binary_target_column,
    validate_features_to_drop,
    validate_native_classification_estimator,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.probe_feature_selection import (
    _extract_feature_importances,
)
from spark_feature_engine.selection.recursive_feature_addition import _score_feature_set
from spark_feature_engine.selection.single_feature_performance import (
    _resolve_numeric_variables,
)


class RecursiveFeatureElimination(BaseSparkEstimator):
    """Learn and remove dispensable features recursively."""

    def __init__(
        self,
        *,
        estimator: Any,
        target: str,
        variables: Sequence[str] | None = None,
        scoring: str = "roc_auc",
        threshold: float = 0.0,
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(target, str) or not target:
            raise TypeError("target must be a non-empty string")
        self._estimator = estimator
        self._target = target
        self._scoring = normalize_classification_scoring(scoring)
        self._threshold = normalize_selector_threshold(threshold, name="threshold")

    def _fit(self, dataset: DataFrame) -> "RecursiveFeatureEliminationModel":
        validate_native_classification_estimator(
            self._estimator,
            require_feature_importance=True,
        )
        target = validate_binary_target_column(dataset, target=self._target)
        variables = _resolve_numeric_variables(
            dataset,
            target=target,
            variables=self.get_variables(),
        )

        from pyspark.ml.feature import VectorAssembler
        from pyspark.sql import functions as F

        assembler = VectorAssembler(inputCols=list(variables), outputCol="features")
        prepared = assembler.transform(dataset).select(
            F.col(target).alias("label"), "features"
        )
        baseline_model = self._estimator.copy({}).fit(prepared)
        feature_importances = _extract_feature_importances(baseline_model, variables)
        ranked_variables = sorted(
            variables,
            key=lambda name: (feature_importances[name], name),
        )

        selected = list(variables)
        baseline_score = _score_feature_set(
            dataset,
            target=target,
            variables=selected,
            estimator=self._estimator,
            scoring=self._scoring,
        )
        performance_drifts: dict[str, float] = {}
        for variable in ranked_variables:
            if len(selected) == 1:
                performance_drifts[variable] = 0.0
                break
            candidate = [name for name in selected if name != variable]
            candidate_score = _score_feature_set(
                dataset,
                target=target,
                variables=candidate,
                estimator=self._estimator,
                scoring=self._scoring,
            )
            drift = baseline_score - candidate_score
            performance_drifts[variable] = float(drift)
            if drift <= self._threshold:
                selected = candidate
                baseline_score = candidate_score

        for variable in variables:
            performance_drifts.setdefault(variable, 0.0)

        features_to_drop = [
            variable for variable in variables if variable not in selected
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return RecursiveFeatureEliminationModel(
            variables_=variables,
            target_=target,
            scoring_=self._scoring,
            threshold_=self._threshold,
            feature_importances_=feature_importances,
            performance_drifts_=performance_drifts,
            features_to_drop_=validated_features_to_drop,
        )


class RecursiveFeatureEliminationModel(BaseSparkModel):
    """Fitted recursive-elimination selector."""

    variables_: list[str]
    target_: str
    scoring_: str
    threshold_: float
    feature_importances_: dict[str, float]
    performance_drifts_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        scoring_: str,
        threshold_: float,
        feature_importances_: dict[str, float],
        performance_drifts_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("scoring_", scoring_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("feature_importances_", dict(feature_importances_))
        self._set_learned_attribute("performance_drifts_", dict(performance_drifts_))
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "scoring_",
            "threshold_",
            "feature_importances_",
            "performance_drifts_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("RecursiveFeatureElimination", "RecursiveFeatureEliminationModel")
