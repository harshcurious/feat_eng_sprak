"""Spark-native single-feature performance selection."""

from __future__ import annotations

from statistics import pstdev
from typing import Any, Sequence

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
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


def _normalize_cv(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 2:
        raise ValueError("cv must be an integer greater than or equal to 2")
    return value


def _resolve_numeric_variables(
    dataset: DataFrame,
    *,
    target: str,
    variables: Sequence[str] | None,
) -> list[str]:
    if variables is None:
        resolved = [
            field.name
            for field in dataset.schema.fields
            if field.name != target
            and field.dataType.simpleString()
            in {"double", "float", "int", "bigint", "smallint", "tinyint", "decimal"}
        ]
    else:
        resolved = [column for column in list(variables) if column != target]
    if not resolved:
        raise ValueError(
            "variables must contain at least one numeric non-target feature"
        )
    return resolved


def _stratified_folds(dataset: DataFrame, *, target: str, cv: int) -> DataFrame:
    ordering = Window.partitionBy(target).orderBy(
        F.xxhash64(*[F.col(column).cast("string") for column in dataset.columns]),
        *[F.col(column) for column in dataset.columns],
    )
    return dataset.withColumn(
        "_fold_id",
        F.pmod(F.row_number().over(ordering) - F.lit(1), F.lit(cv)).cast("int"),
    )


def _clone_estimator(estimator: Any) -> Any:
    cloned = estimator.copy({})
    if hasattr(cloned, "hasParam") and cloned.hasParam("labelCol"):
        cloned = cloned.copy({cloned.getParam("labelCol"): "label"})
    if hasattr(cloned, "hasParam") and cloned.hasParam("featuresCol"):
        cloned = cloned.copy({cloned.getParam("featuresCol"): "features"})
    return cloned


def _evaluate_predictions(predictions: DataFrame, *, scoring: str) -> float:
    evaluator: Any
    if scoring == "accuracy":
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        )
    else:
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
    return float(evaluator.evaluate(predictions))


class SelectBySingleFeaturePerformance(BaseSparkEstimator):
    """Learn and drop features with weak individual predictive performance."""

    def __init__(
        self,
        *,
        estimator: Any,
        target: str,
        variables: Sequence[str] | None = None,
        scoring: str = "roc_auc",
        threshold: float | None = None,
        cv: int = 3,
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

    def _fit(self, dataset: DataFrame) -> "SelectBySingleFeaturePerformanceModel":
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

        feature_performance: dict[str, float] = {}
        feature_performance_std: dict[str, float] = {}
        for variable in variables:
            scores: list[float] = []
            assembler = VectorAssembler(inputCols=[variable], outputCol="features")
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
                predictions = model.transform(valid_prepared)
                scores.append(_evaluate_predictions(predictions, scoring=self._scoring))

            feature_performance[variable] = float(sum(scores) / len(scores))
            feature_performance_std[variable] = float(pstdev(scores))

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

        return SelectBySingleFeaturePerformanceModel(
            variables_=variables,
            target_=target,
            scoring_=self._scoring,
            threshold_=threshold,
            cv_=self._cv,
            feature_performance_=feature_performance,
            feature_performance_std_=feature_performance_std,
            features_to_drop_=validated_features_to_drop,
        )


class SelectBySingleFeaturePerformanceModel(BaseSparkModel):
    """Fitted single-feature performance selector."""

    variables_: list[str]
    target_: str
    scoring_: str
    threshold_: float
    cv_: int
    feature_performance_: dict[str, float]
    feature_performance_std_: dict[str, float]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        scoring_: str,
        threshold_: float,
        cv_: int,
        feature_performance_: dict[str, float],
        feature_performance_std_: dict[str, float],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("scoring_", scoring_)
        self._set_learned_attribute("threshold_", threshold_)
        self._set_learned_attribute("cv_", cv_)
        self._set_learned_attribute("feature_performance_", dict(feature_performance_))
        self._set_learned_attribute(
            "feature_performance_std_", dict(feature_performance_std_)
        )
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "scoring_",
            "threshold_",
            "cv_",
            "feature_performance_",
            "feature_performance_std_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = (
    "SelectBySingleFeaturePerformance",
    "SelectBySingleFeaturePerformanceModel",
)
