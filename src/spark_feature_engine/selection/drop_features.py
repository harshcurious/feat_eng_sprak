"""Spark-native configured feature dropper."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame

from spark_feature_engine._validation import (
    to_optional_list_of_strings,
    validate_configured_features_to_drop,
)
from spark_feature_engine.base import BaseSparkTransformer


class DropFeatures(BaseSparkTransformer):
    """Drop explicitly configured columns from a dataset."""

    def __init__(self, *, features_to_drop: Sequence[str]) -> None:
        super().__init__(variables=None)
        try:
            normalized = to_optional_list_of_strings(features_to_drop)
        except TypeError as error:
            raise TypeError(
                "features_to_drop must be a sequence of column names, not a string"
            ) from error
        if normalized is None or not normalized:
            raise ValueError("features_to_drop must contain at least one column name")
        self._features_to_drop = normalized

    def _transform(self, dataset: DataFrame) -> DataFrame:
        features_to_drop = validate_configured_features_to_drop(
            dataset_columns=dataset.columns,
            features_to_drop=self._features_to_drop,
        )
        return dataset.drop(*features_to_drop)


__all__ = ("DropFeatures",)
