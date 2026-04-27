"""Spark-native shuffle-based feature selection."""

from __future__ import annotations

from statistics import pstdev
from typing import Any, Sequence

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_classification_scoring,
    normalize_selector_threshold,
    validate_binary_target_column,
    validate_features_to_drop,
    validate_native_classification_estimator,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.single_feature_performance import (
    _clone_estimator,
    _evaluate_predictions,
    _normalize_cv,
    _resolve_numeric_variables,
    _stratified_folds,
)


def _normalize_random_state(value: int | None) -> int:
    if value is None:
        return 0
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("random_state must be an integer or None")
    return value


def _shuffle_validation_feature(
    dataset: DataFrame,
    *,
    variable: str,
    seed: int,
) -> DataFrame:
    identity_window = Window.orderBy(F.monotonically_increasing_id())
    shuffled_window = Window.orderBy(F.rand(seed))
    indexed = dataset.withColumn(
        "_shuffle_row_id", F.row_number().over(identity_window)
    )
    shuffled = dataset.select(variable).withColumn(
        "_shuffle_row_id",
        F.row_number().over(shuffled_window),
    )
    return (
        indexed.drop(variable)
        .join(shuffled, on="_shuffle_row_id", how="inner")
        .drop("_shuffle_row_id")
    )


class SelectByShuffling(BaseSparkEstimator):
    """Learn and drop features with low shuffle importance."""

    def __init__(
        self,
        *,
        estimator: Any,
        target: str,
        variables: Sequence[str] | None = None,
        scoring: str = "roc_auc",
        threshold: float | None = None,
        cv: int = 3,
        random_state: int | None = None,
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(target, str) or not target:
            raise TypeError("target must be a non-empty string")
        self._estimator = estimator
        self._target = target
        self._scoring = normalize_classification_scoring(scoring)
        self._threshold = (
            None
            if threshold is None
            else normalize_selector_threshold(threshold, name="threshold")
        )
        self._cv = _normalize_cv(cv)
        self._random_state = _normalize_random_state(random_state)

    def _fit(self, dataset: DataFrame) -> "SelectByShufflingModel":
        validate_native_classification_estimator(self._estimator)
        target = validate_binary_target_column(dataset, target=self._target)
        variables = _resolve_numeric_variables(
            dataset,
            target=target,
            variables=self.get_variables(),
        )
        folded = _stratified_folds(
            dataset.select(target, *variables), target=target, cv=self._cv
        )

        baseline_scores: list[float] = []
        drifts: dict[str, list[float]] = {variable: [] for variable in variables}
        assembler = VectorAssembler(inputCols=list(variables), outputCol="features")
        for fold_id in range(self._cv):
            train = folded.where(F.col("_fold_id") != F.lit(fold_id))
            valid = folded.where(F.col("_fold_id") == F.lit(fold_id))
            train_prepared = assembler.transform(train).select(
                F.col(target).alias("label"), "features"
            )
            valid_prepared = assembler.transform(valid).select(
                F.col(target).alias("label"), "features"
            )
            model = _clone_estimator(self._estimator).fit(train_prepared)
            baseline_predictions = model.transform(valid_prepared)
            baseline_score = _evaluate_predictions(
                baseline_predictions,
                scoring=self._scoring,
            )
            baseline_scores.append(baseline_score)

            for variable in variables:
                shuffled_valid = _shuffle_validation_feature(
                    valid.select(target, *variables),
                    variable=variable,
                    seed=self._random_state + fold_id,
                )
                shuffled_prepared = assembler.transform(shuffled_valid).select(
                    F.col(target).alias("label"), "features"
                )
                shuffled_predictions = model.transform(shuffled_prepared)
                shuffled_score = _evaluate_predictions(
                    shuffled_predictions,
                    scoring=self._scoring,
                )
                drifts[variable].append(baseline_score - shuffled_score)

        initial_model_performance = float(sum(baseline_scores) / len(baseline_scores))
        performance_drifts = {
            variable: float(sum(values) / len(values))
            for variable, values in drifts.items()
        }
        performance_drifts_std = {
            variable: float(pstdev(values)) for variable, values in drifts.items()
        }
        threshold = (
            self._threshold
            if self._threshold is not None
            else sum(performance_drifts.values()) / float(len(performance_drifts))
        )
        features_to_drop = [
            variable
            for variable, drift in performance_drifts.items()
            if drift < threshold
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return SelectByShufflingModel(
            variables_=variables,
            target_=target,
            scoring_=self._scoring,
            threshold_=threshold,
            cv_=self._cv,
            random_state_=self._random_state,
            initial_model_performance_=initial_model_performance,
            performance_drifts_=performance_drifts,
            performance_drifts_std_=performance_drifts_std,
            features_to_drop_=validated_features_to_drop,
        )


class SelectByShufflingModel(BaseSparkModel):
    """Fitted shuffle-based selector."""

    variables_: list[str]
    target_: str
    scoring_: str
    threshold_: float
    cv_: int
    random_state_: int
    initial_model_performance_: float
    performance_drifts_: dict[str, float]
    performance_drifts_std_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        scoring_: str,
        threshold_: float,
        cv_: int,
        random_state_: int,
        initial_model_performance_: float,
        performance_drifts_: dict[str, float],
        performance_drifts_std_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("scoring_", scoring_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("cv_", cv_)
        self._set_learned_attribute("random_state_", random_state_)
        self._set_learned_attribute(
            "initial_model_performance_", initial_model_performance_
        )
        self._set_learned_attribute("performance_drifts_", dict(performance_drifts_))
        self._set_learned_attribute(
            "performance_drifts_std_", dict(performance_drifts_std_)
        )
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "scoring_",
            "threshold_",
            "cv_",
            "random_state_",
            "initial_model_performance_",
            "performance_drifts_",
            "performance_drifts_std_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("SelectByShuffling", "SelectByShufflingModel")
