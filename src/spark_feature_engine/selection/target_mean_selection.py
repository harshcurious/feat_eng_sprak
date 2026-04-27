"""Spark-native target-mean performance feature selection."""

from __future__ import annotations

from statistics import pstdev
from typing import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_classification_scoring,
    normalize_selector_threshold,
    resolve_variables,
    validate_bin_count,
    validate_binary_target_column,
    validate_features_to_drop,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.drop_psi_features import _bucketed_dataset

_SUPPORTED_STRATEGIES = ("equal_width", "equal_frequency")


def _normalize_strategy(value: str) -> str:
    return validate_supported_option("strategy", value, allowed=_SUPPORTED_STRATEGIES)


def _normalize_cv(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 2:
        raise ValueError("cv must be an integer greater than or equal to 2")
    return value


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


def _roc_auc_score(labels: list[int], scores: list[float]) -> float:
    positives = sum(1 for label in labels if label == 1)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return 0.5

    ordered = sorted(zip(scores, labels), key=lambda item: item[0])
    rank_sum = 0.0
    for index, (_, label) in enumerate(ordered, start=1):
        if label == 1:
            rank_sum += index
    return float(
        (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)
    )


def _score_predictions(
    labels: list[int], scores: list[float], *, scoring: str
) -> float:
    if scoring == "accuracy":
        predictions = [1 if score >= 0.5 else 0 for score in scores]
        matches = sum(
            int(label == prediction) for label, prediction in zip(labels, predictions)
        )
        return float(matches) / float(len(labels))
    return _roc_auc_score(labels, scores)


def _mean_encoding_score(
    dataset: DataFrame,
    *,
    target: str,
    scoring: str,
    cv: int,
) -> tuple[float, float]:
    ordering = Window.partitionBy(target).orderBy(
        F.xxhash64(F.lit(0), F.col(target).cast("string"), F.col("_bucket")),
        F.col("_bucket"),
    )
    folded = dataset.withColumn(
        "_fold_id",
        F.pmod(F.row_number().over(ordering) - F.lit(1), F.lit(cv)).cast("int"),
    )
    fold_scores: list[float] = []
    for fold_id in range(cv):
        training = folded.where(F.col("_fold_id") != F.lit(fold_id)).cache()
        validation = folded.where(F.col("_fold_id") == F.lit(fold_id)).cache()
        if training.limit(1).count() == 0 or validation.limit(1).count() == 0:
            training.unpersist()
            validation.unpersist()
            continue

        global_mean = training.agg(F.avg(target).alias("global_mean")).collect()[0][
            "global_mean"
        ]
        mapping = training.groupBy("_bucket").agg(F.avg(target).alias("encoded_value"))
        scored = (
            validation.join(mapping, on="_bucket", how="left")
            .withColumn(
                "encoded_value", F.coalesce(F.col("encoded_value"), F.lit(global_mean))
            )
            .select(target, "encoded_value")
            .collect()
        )
        labels = [int(row[target]) for row in scored]
        scores = [float(row["encoded_value"]) for row in scored]
        fold_scores.append(_score_predictions(labels, scores, scoring=scoring))
        training.unpersist()
        validation.unpersist()

    if not fold_scores:
        return 0.0, 0.0
    return float(sum(fold_scores) / len(fold_scores)), float(pstdev(fold_scores))


class SelectByTargetMeanPerformance(BaseSparkEstimator):
    """Learn and drop features below a target-mean performance threshold."""

    def __init__(
        self,
        *,
        target: str,
        variables: Sequence[str] | None = None,
        bins: int = 5,
        strategy: str = "equal_width",
        scoring: str = "roc_auc",
        cv: int = 3,
        threshold: float | None = None,
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
        self._scoring = normalize_classification_scoring(scoring)
        self._cv = _normalize_cv(cv)
        self._threshold = (
            None
            if threshold is None
            else normalize_selector_threshold(threshold, name="threshold")
        )

    def _fit(self, dataset: DataFrame) -> "SelectByTargetMeanPerformanceModel":
        target = validate_binary_target_column(dataset, target=self._target)
        variables = _resolve_selection_variables(
            dataset,
            target=target,
            variables=self.get_variables(),
        )

        feature_performance: dict[str, float] = {}
        feature_performance_std: dict[str, float] = {}
        for variable in variables:
            bucketed = _bucketed_dataset(
                dataset.select(target, variable).where(F.col(variable).isNotNull()),
                variable=variable,
                target=target,
                bins=self._bins,
                strategy=self._strategy,
            )
            score, score_std = _mean_encoding_score(
                bucketed,
                target=target,
                scoring=self._scoring,
                cv=self._cv,
            )
            feature_performance[variable] = score
            feature_performance_std[variable] = score_std

        threshold = (
            self._threshold
            if self._threshold is not None
            else sum(feature_performance.values()) / float(len(feature_performance))
        )
        features_to_drop = [
            variable
            for variable, score in feature_performance.items()
            if score < threshold
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return SelectByTargetMeanPerformanceModel(
            variables_=variables,
            target_=target,
            bins_=self._bins,
            strategy_=self._strategy,
            scoring_=self._scoring,
            cv_=self._cv,
            threshold_=threshold,
            feature_performance_=feature_performance,
            feature_performance_std_=feature_performance_std,
            features_to_drop_=validated_features_to_drop,
        )


class SelectByTargetMeanPerformanceModel(BaseSparkModel):
    """Fitted target-mean performance selector."""

    variables_: list[str]
    target_: str
    bins_: int
    strategy_: str
    scoring_: str
    cv_: int
    threshold_: float
    feature_performance_: dict[str, float]
    feature_performance_std_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        bins_: int,
        strategy_: str,
        scoring_: str,
        cv_: int,
        threshold_: float,
        feature_performance_: dict[str, float],
        feature_performance_std_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("bins_", bins_)
        self._set_learned_attribute("strategy_", strategy_)
        self._set_learned_attribute("scoring_", scoring_)
        self._set_learned_attribute("cv_", cv_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("feature_performance_", dict(feature_performance_))
        self._set_learned_attribute(
            "feature_performance_std_", dict(feature_performance_std_)
        )
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "bins_",
            "strategy_",
            "scoring_",
            "cv_",
            "threshold_",
            "feature_performance_",
            "feature_performance_std_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = (
    "SelectByTargetMeanPerformance",
    "SelectByTargetMeanPerformanceModel",
)
