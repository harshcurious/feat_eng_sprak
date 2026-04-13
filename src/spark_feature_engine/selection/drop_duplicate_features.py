"""Spark-native duplicate feature selection."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_variables,
    validate_features_to_drop,
    validate_minimum_variable_count,
    validate_supported_option,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_MISSING_VALUE_POLICIES = ("raise", "ignore")


def _normalize_missing_values(value: str) -> str:
    return validate_supported_option(
        "missing_values",
        value,
        allowed=_MISSING_VALUE_POLICIES,
    )


def _column_signature(
    dataset: DataFrame, column_name: str
) -> tuple[tuple[Any, int], ...]:
    summary = dataset.groupBy(F.col(column_name)).count().collect()
    normalized_rows = [(row[column_name], int(row["count"])) for row in summary]
    normalized_rows.sort(key=lambda item: (item[0] is not None, repr(item[0]), item[1]))
    return tuple(normalized_rows)


class DropDuplicateFeatures(BaseSparkEstimator):
    """Learn and drop exact duplicate features."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        missing_values: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        self._missing_values = _normalize_missing_values(missing_values)

    def _fit(self, dataset: DataFrame) -> "DropDuplicateFeaturesModel":
        variables = resolve_variables(dataset, variables=self.get_variables())
        validate_minimum_variable_count(variables, minimum=2, name="variables")

        if self._missing_values == "raise":
            for variable in variables:
                if dataset.where(F.col(variable).isNull()).limit(1).count():
                    raise ValueError(f"Variable '{variable}' contains missing values")

        grouped_variables: dict[tuple[tuple[Any, int], ...], list[str]] = defaultdict(
            list
        )
        for variable in variables:
            grouped_variables[_column_signature(dataset, variable)].append(variable)

        duplicated_feature_sets = [
            duplicate_group
            for duplicate_group in grouped_variables.values()
            if len(duplicate_group) > 1
        ]
        features_to_drop = [
            duplicate
            for duplicate_group in duplicated_feature_sets
            for duplicate in duplicate_group[1:]
        ]
        validated_features_to_drop = validate_features_to_drop(
            variables=dataset.columns,
            features_to_drop=features_to_drop,
        )

        return DropDuplicateFeaturesModel(
            variables_=variables,
            missing_values_=self._missing_values,
            duplicated_feature_sets_=duplicated_feature_sets,
            features_to_drop_=validated_features_to_drop,
        )


class DropDuplicateFeaturesModel(BaseSparkModel):
    """Fitted duplicate-feature selector."""

    variables_: list[str]
    missing_values_: str
    duplicated_feature_sets_: list[list[str]]
    features_to_drop_: list[str]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        missing_values_: str,
        duplicated_feature_sets_: Sequence[Sequence[str]],
        features_to_drop_: Sequence[str],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("missing_values_", missing_values_)
        self._set_learned_attribute(
            "duplicated_feature_sets_",
            [list(group) for group in duplicated_feature_sets_],
        )
        self._set_learned_attribute("features_to_drop_", list(features_to_drop_))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "missing_values_",
            "duplicated_feature_sets_",
            "features_to_drop_",
        )
        return dataset.drop(*self.features_to_drop_)


__all__ = ("DropDuplicateFeatures", "DropDuplicateFeaturesModel")
