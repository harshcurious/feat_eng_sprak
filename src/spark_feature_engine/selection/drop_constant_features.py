"""Spark-native constant and quasi-constant feature selection."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    normalize_selector_threshold,
    resolve_variables,
    to_optional_list_of_strings,
    validate_features_to_drop,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_MISSING_VALUE_POLICIES = ("raise", "ignore", "include")


def _normalize_missing_values(value: str) -> str:
    return validate_supported_option(
        "missing_values",
        value,
        allowed=_MISSING_VALUE_POLICIES,
    )


class DropConstantFeatures(BaseSparkEstimator):
    """Learn and drop constant or quasi-constant features."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        tol: float = 1.0,
        missing_values: str = "raise",
    ) -> None:
        super().__init__(variables=variables)
        self._tol = normalize_selector_threshold(tol, name="tol")
        self._missing_values = _normalize_missing_values(missing_values)

    def _fit(self, dataset: DataFrame) -> "DropConstantFeaturesModel":
        variables = resolve_variables(dataset, variables=self.get_variables())
        features_to_drop: list[str] = []

        for variable in variables:
            if self._missing_values == "raise":
                null_count = dataset.where(F.col(variable).isNull()).limit(1).count()
                if null_count:
                    raise ValueError(f"Variable '{variable}' contains missing values")

            base_dataset = (
                dataset
                if self._missing_values == "include"
                else dataset.where(F.col(variable).isNotNull())
            )
            total_count = base_dataset.count()
            if total_count == 0:
                predominant_frequency = 1.0
            else:
                top_row = (
                    base_dataset.groupBy(F.col(variable))
                    .count()
                    .orderBy(F.desc("count"), F.asc_nulls_first(variable))
                    .limit(1)
                    .collect()[0]
                )
                predominant_frequency = float(top_row["count"]) / float(total_count)

            if predominant_frequency >= self._tol:
                features_to_drop.append(variable)

        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return DropConstantFeaturesModel(
            variables_=variables,
            tol_=self._tol,
            missing_values_=self._missing_values,
            features_to_drop_=validated_features_to_drop,
        )


class DropConstantFeaturesModel(BaseSparkModel):
    """Fitted constant-feature selector."""

    variables_: list[str]
    tol_: float
    missing_values_: str
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        tol_: float,
        missing_values_: str,
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("tol_", tol_)
        self._set_learned_attribute("missing_values_", missing_values_)
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "tol_",
            "missing_values_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("DropConstantFeatures", "DropConstantFeaturesModel")
