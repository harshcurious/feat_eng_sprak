"""Spark-native logarithm transform for numeric variables."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import resolve_numeric_columns
from spark_feature_engine.base import BaseSparkTransformer


def _log_expression(column_name: str) -> Column:
    return F.log(F.col(column_name)).alias(column_name)


class LogTransformer(BaseSparkTransformer):
    """Apply the natural log to selected numeric columns in place."""

    def _transform(self, dataset: DataFrame) -> DataFrame:
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())

        invalid_columns: list[str] = []
        for column_name in variables:
            minimum = dataset.select(
                F.min(F.col(column_name)).alias(column_name)
            ).first()
            assert minimum is not None
            value = minimum[column_name]
            if value is not None and float(value) <= 0.0:
                invalid_columns.append(column_name)

        if invalid_columns:
            joined = ", ".join(invalid_columns)
            raise ValueError(
                f"Log transform requires strictly positive values: {joined}"
            )

        projections: list[Column] = []
        for column_name in dataset.columns:
            if column_name not in variables:
                projections.append(F.col(column_name))
                continue
            projections.append(_log_expression(column_name))
        return dataset.select(*projections)


__all__ = ["LogTransformer"]
