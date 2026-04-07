"""Row-dropping transformer for selected missing values."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_feature_engine.base import BaseSparkTransformer


class DropMissingData(BaseSparkTransformer):
    """Remove rows containing nulls in the selected columns."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        selected_columns = self.resolve_variables(dataset)
        if not selected_columns:
            return dataset
        return dataset.na.drop(subset=selected_columns)


__all__ = ("DropMissingData",)
