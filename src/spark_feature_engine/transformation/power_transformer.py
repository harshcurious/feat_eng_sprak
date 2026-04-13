"""Spark-native power transform for numeric variables."""

from __future__ import annotations

from numbers import Real
from typing import Any, Sequence, cast

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import normalize_exponent, resolve_numeric_columns
from spark_feature_engine.base import BaseSparkTransformer


class PowerTransformer(BaseSparkTransformer):
    """Raise selected numeric columns to a power in place."""

    def __init__(
        self, *, variables: Sequence[str] | None = None, exponent: float = 2.0
    ) -> None:
        super().__init__(variables=variables)
        self.exponent = exponent

    def _transform(self, dataset: DataFrame) -> DataFrame:
        exponent = normalize_exponent(cast(Any, self.exponent))
        variables = resolve_numeric_columns(dataset, variables=self.get_variables())

        projections = []
        for column_name in dataset.columns:
            if column_name not in variables:
                projections.append(F.col(column_name))
                continue
            projections.append(
                F.pow(F.col(column_name), F.lit(exponent)).alias(column_name)
            )
        return dataset.select(*projections)


__all__ = ["PowerTransformer"]
