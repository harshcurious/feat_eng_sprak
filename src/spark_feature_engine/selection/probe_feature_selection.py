"""Spark-native collective-mode probe feature selection."""

from __future__ import annotations

from statistics import mean, pstdev
from typing import Any, Sequence

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    validate_binary_target_column,
    validate_features_to_drop,
    validate_native_classification_estimator,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel
from spark_feature_engine.selection.single_feature_performance import (
    _resolve_numeric_variables,
)

_SUPPORTED_DISTRIBUTIONS = (
    "normal",
    "binary",
    "uniform",
    "discrete_uniform",
    "poisson",
    "all",
)
_SUPPORTED_THRESHOLDS = ("mean", "max", "mean_plus_std")


def _normalize_n_probes(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("n_probes must be a positive integer")
    return value


def _normalize_random_state(value: int | None) -> int:
    if value is None:
        return 0
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("random_state must be an integer or None")
    return value


def _normalize_distribution(value: str) -> str:
    return validate_supported_option(
        "distribution",
        value,
        allowed=_SUPPORTED_DISTRIBUTIONS,
    )


def _normalize_threshold_rule(value: str) -> str:
    return validate_supported_option(
        "threshold",
        value,
        allowed=_SUPPORTED_THRESHOLDS,
    )


def _probe_names(*, distribution: str, n_probes: int) -> list[str]:
    distributions = (
        [distribution]
        if distribution != "all"
        else ["normal", "binary", "uniform", "discrete_uniform", "poisson"]
    )
    return [
        f"__probe_{distribution_name}_{index}"
        for distribution_name in distributions
        for index in range(n_probes)
    ]


def _with_probe_columns(
    dataset: DataFrame,
    *,
    distribution: str,
    n_probes: int,
    random_state: int,
    n_categories: int,
) -> tuple[DataFrame, list[str]]:
    distributions = (
        [distribution]
        if distribution != "all"
        else ["normal", "binary", "uniform", "discrete_uniform", "poisson"]
    )
    result = dataset
    probe_columns: list[str] = []
    seed = random_state
    for distribution_name in distributions:
        for index in range(n_probes):
            probe_name = f"__probe_{distribution_name}_{index}"
            if distribution_name == "normal":
                expression = F.randn(seed)
            elif distribution_name == "binary":
                expression = (F.rand(seed) >= F.lit(0.5)).cast("double")
            elif distribution_name == "uniform":
                expression = F.rand(seed)
            elif distribution_name == "discrete_uniform":
                expression = F.floor(F.rand(seed) * F.lit(n_categories)).cast("double")
            else:
                expression = F.floor(
                    -F.log1p(-F.rand(seed)) * F.lit(float(n_categories))
                ).cast("double")
            result = result.withColumn(probe_name, expression)
            probe_columns.append(probe_name)
            seed += 1
    return result, probe_columns


def _extract_feature_importances(
    model: Any, feature_names: Sequence[str]
) -> dict[str, float]:
    if hasattr(model, "featureImportances"):
        importances = [
            float(value) for value in model.featureImportances.toArray().tolist()
        ]
    elif hasattr(model, "coefficients"):
        importances = [
            abs(float(value)) for value in model.coefficients.toArray().tolist()
        ]
    else:
        raise TypeError("Estimator model must expose feature importance metadata")
    return dict(zip(feature_names, importances, strict=True))


def _probe_threshold(values: Sequence[float], *, rule: str) -> float:
    if rule == "mean":
        return float(mean(values))
    if rule == "max":
        return float(max(values))
    return float(mean(values) + pstdev(values))


class ProbeFeatureSelection(BaseSparkEstimator):
    """Learn and drop weak features relative to generated probe features."""

    def __init__(
        self,
        *,
        estimator: Any,
        target: str,
        variables: Sequence[str] | None = None,
        n_probes: int = 1,
        distribution: str = "normal",
        threshold: str = "mean",
        n_categories: int = 10,
        random_state: int | None = None,
    ) -> None:
        super().__init__(variables=variables)
        if not isinstance(target, str) or not target:
            raise TypeError("target must be a non-empty string")
        self._estimator = estimator
        self._target = target
        self._n_probes = _normalize_n_probes(n_probes)
        self._distribution = _normalize_distribution(distribution)
        self._threshold = _normalize_threshold_rule(threshold)
        self._n_categories = _normalize_n_probes(n_categories)
        self._random_state = _normalize_random_state(random_state)

    def _fit(self, dataset: DataFrame) -> "ProbeFeatureSelectionModel":
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
        augmented, probe_features = _with_probe_columns(
            dataset.select(target, *variables),
            distribution=self._distribution,
            n_probes=self._n_probes,
            random_state=self._random_state,
            n_categories=self._n_categories,
        )
        feature_names = [*variables, *probe_features]
        assembler = VectorAssembler(inputCols=feature_names, outputCol="features")
        prepared = assembler.transform(augmented).select(
            F.col(target).alias("label"),
            "features",
        )
        model = self._estimator.copy({}).fit(prepared)
        importances = _extract_feature_importances(model, feature_names)
        probe_importances = [importances[name] for name in probe_features]
        threshold = _probe_threshold(probe_importances, rule=self._threshold)
        features_to_drop = [
            variable for variable in variables if importances[variable] <= threshold
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return ProbeFeatureSelectionModel(
            variables_=variables,
            target_=target,
            probe_features_=probe_features,
            feature_importances_={name: importances[name] for name in variables},
            probe_importances_={name: importances[name] for name in probe_features},
            probe_threshold_=threshold,
            features_to_drop_=validated_features_to_drop,
            distribution_=self._distribution,
            n_probes_=self._n_probes,
            threshold_rule_=self._threshold,
            random_state_=self._random_state,
        )


class ProbeFeatureSelectionModel(BaseSparkModel):
    """Fitted collective-mode probe selector."""

    variables_: list[str]
    target_: str
    probe_features_: list[str]
    feature_importances_: dict[str, float]
    probe_importances_: dict[str, float]
    probe_threshold_: float
    features_to_drop_: list[str]
    distribution_: str
    n_probes_: int
    threshold_rule_: str
    random_state_: int

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        target_: str,
        probe_features_: Sequence[str],
        feature_importances_: dict[str, float],
        probe_importances_: dict[str, float],
        probe_threshold_: float,
        features_to_drop_: Sequence[str],
        distribution_: str,
        n_probes_: int,
        threshold_rule_: str,
        random_state_: int,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("target_", target_)
        self._set_learned_attribute("probe_features_", list(probe_features_))
        self._set_learned_attribute("feature_importances_", dict(feature_importances_))
        self._set_learned_attribute("probe_importances_", dict(probe_importances_))
        self._set_learned_attribute("probe_threshold_", probe_threshold_)
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))
        self._set_learned_attribute("distribution_", distribution_)
        self._set_learned_attribute("n_probes_", n_probes_)
        self._set_learned_attribute("threshold_rule_", threshold_rule_)
        self._set_learned_attribute("random_state_", random_state_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "target_",
            "probe_features_",
            "feature_importances_",
            "probe_importances_",
            "probe_threshold_",
            "features_to_drop_",
            "distribution_",
            "n_probes_",
            "threshold_rule_",
            "random_state_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("ProbeFeatureSelection", "ProbeFeatureSelectionModel")
