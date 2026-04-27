"""Spark-native recursive feature addition."""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, functions as F

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
from spark_feature_engine.selection.single_feature_performance import (
    _clone_estimator,
    _evaluate_predictions,
    _resolve_numeric_variables,
)


def _score_feature_set(
    dataset: DataFrame,
    *,
    target: str,
    variables: Sequence[str],
    estimator: Any,
    scoring: str,
) -> float:
    assembler = VectorAssembler(inputCols=list(variables), outputCol="features")
    prepared = assembler.transform(dataset).select(
        F.col(target).alias("label"), "features"
    )
    model = _clone_estimator(estimator).fit(prepared)
    predictions = model.transform(prepared)
    return _evaluate_predictions(predictions, scoring=scoring)


class RecursiveFeatureAddition(BaseSparkEstimator):
    """Learn and keep only incrementally useful features."""

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

    def _fit(self, dataset: DataFrame) -> "RecursiveFeatureAdditionModel":
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

        assembler = VectorAssembler(inputCols=list(variables), outputCol="features")
        prepared = assembler.transform(dataset).select(
            F.col(target).alias("label"), "features"
        )
        baseline_model = _clone_estimator(self._estimator).fit(prepared)
        feature_importances = _extract_feature_importances(baseline_model, variables)
        ranked_variables = sorted(
            variables,
            key=lambda name: (-feature_importances[name], name),
        )

        selected = [ranked_variables[0]]
        performance_drifts = {ranked_variables[0]: 0.0}
        baseline_score = _score_feature_set(
            dataset,
            target=target,
            variables=selected,
            estimator=self._estimator,
            scoring=self._scoring,
        )

        for variable in ranked_variables[1:]:
            candidate = [*selected, variable]
            candidate_score = _score_feature_set(
                dataset,
                target=target,
                variables=candidate,
                estimator=self._estimator,
                scoring=self._scoring,
            )
            drift = candidate_score - baseline_score
            performance_drifts[variable] = float(drift)
            if drift > self._threshold:
                selected.append(variable)
                baseline_score = candidate_score

        features_to_drop = [
            variable for variable in variables if variable not in selected
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return RecursiveFeatureAdditionModel(
            variables_=variables,
            target_=target,
            scoring_=self._scoring,
            threshold_=self._threshold,
            feature_importances_=feature_importances,
            performance_drifts_=performance_drifts,
            features_to_drop_=validated_features_to_drop,
        )


class RecursiveFeatureAdditionModel(BaseSparkModel):
    """Fitted recursive-addition selector."""

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


__all__ = ("RecursiveFeatureAddition", "RecursiveFeatureAdditionModel")
