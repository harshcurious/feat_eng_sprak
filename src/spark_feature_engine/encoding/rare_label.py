"""Spark-native learned rare-label encoding."""

from __future__ import annotations

from numbers import Real
from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_categorical_columns,
    validate_column_presence,
    validate_column_types,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel


class RareLabelEncoder(BaseSparkEstimator):
    """Learn frequent labels and group infrequent categories."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        tolerance: float = 0.05,
        min_categories: int = 10,
        max_categories: int | None = None,
        replacement_label: str = "Rare",
    ) -> None:
        super().__init__(variables=variables)
        self.tolerance = _validate_tolerance(tolerance)
        self.min_categories = _validate_positive_int(
            min_categories,
            name="min_categories",
        )
        self.max_categories = _validate_optional_positive_int(
            max_categories,
            name="max_categories",
        )
        self.replacement_label = _validate_replacement_label(replacement_label)

    def _fit(self, dataset: DataFrame) -> "RareLabelEncoderModel":
        variables = resolve_categorical_columns(dataset, variables=self.get_variables())
        frequent_labels = _learn_frequent_labels(
            dataset,
            variables=variables,
            tolerance=self.tolerance,
            min_categories=self.min_categories,
            max_categories=self.max_categories,
        )

        return RareLabelEncoderModel(
            variables_=list(variables),
            tolerance_=self.tolerance,
            min_categories_=self.min_categories,
            max_categories_=self.max_categories,
            replacement_label_=self.replacement_label,
            frequent_labels_=frequent_labels,
        )


class RareLabelEncoderModel(BaseSparkModel):
    """Fitted rare-label encoder backed by native Spark expressions."""

    variables_: list[str]
    tolerance_: float
    min_categories_: int
    max_categories_: int | None
    replacement_label_: str
    frequent_labels_: dict[str, list[str]]

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        tolerance_: float,
        min_categories_: int,
        max_categories_: int | None,
        replacement_label_: str,
        frequent_labels_: dict[str, list[str]],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute("tolerance_", tolerance_)
        self._set_learned_attribute("min_categories_", min_categories_)
        self._set_learned_attribute("max_categories_", max_categories_)
        self._set_learned_attribute("replacement_label_", replacement_label_)
        self._set_learned_attribute(
            "frequent_labels_",
            {variable: list(labels) for variable, labels in frequent_labels_.items()},
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted(
            "variables_",
            "tolerance_",
            "min_categories_",
            "max_categories_",
            "replacement_label_",
            "frequent_labels_",
        )
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="string")

        transformed = dataset
        for variable in self.variables_:
            frequent_labels = self.frequent_labels_[variable]
            transformed = transformed.withColumn(
                variable,
                _replace_rare_labels(
                    variable,
                    frequent_labels=frequent_labels,
                    replacement_label=self.replacement_label_,
                ),
            )
        return transformed


def _validate_tolerance(tolerance: float) -> float:
    if not isinstance(tolerance, Real) or isinstance(tolerance, bool):
        raise TypeError(
            "tolerance must be a numeric value greater than 0 and at most 1"
        )

    normalized = float(tolerance)
    if normalized <= 0.0 or normalized > 1.0:
        raise ValueError("tolerance must be greater than 0 and at most 1")
    return normalized


def _validate_positive_int(value: int, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


def _validate_optional_positive_int(value: int | None, *, name: str) -> int | None:
    if value is None:
        return None
    return _validate_positive_int(value, name=name)


def _validate_replacement_label(replacement_label: str) -> str:
    if not isinstance(replacement_label, str):
        raise TypeError("replacement_label must be a string")
    if not replacement_label:
        raise ValueError("replacement_label must not be empty")
    return replacement_label


def _learn_frequent_labels(
    dataset: DataFrame,
    *,
    variables: Sequence[str],
    tolerance: float,
    min_categories: int,
    max_categories: int | None,
) -> dict[str, list[str]]:
    total_rows = dataset.count()
    denominator = float(total_rows)
    frequent_labels: dict[str, list[str]] = {}

    for variable in variables:
        rows = (
            dataset.where(F.col(variable).isNotNull())
            .groupBy(F.col(variable).alias("category"))
            .agg(F.count(F.lit(1)).alias("count"))
            .orderBy(F.col("count").desc(), F.col("category").asc())
            .collect()
        )

        retained = [
            row.category
            for row in rows
            if denominator > 0.0 and (float(row["count"]) / denominator) >= tolerance
        ]

        categories = [row.category for row in rows]
        if not retained and len(categories) <= min_categories:
            frequent_labels[variable] = sorted(categories)
            continue

        if max_categories is not None:
            frequent_labels[variable] = retained[:max_categories]
        else:
            frequent_labels[variable] = sorted(retained)

    return frequent_labels


def _replace_rare_labels(
    variable: str,
    *,
    frequent_labels: Sequence[str],
    replacement_label: str,
) -> Column:
    column = F.col(variable)
    if frequent_labels:
        return (
            F.when(column.isNull(), F.lit(None))
            .when(column.isin(list(frequent_labels)), column)
            .otherwise(F.lit(replacement_label))
            .alias(variable)
        )

    return (
        F.when(column.isNull(), F.lit(None))
        .otherwise(F.lit(replacement_label))
        .alias(variable)
    )


__all__ = ("RareLabelEncoder", "RareLabelEncoderModel")
