"""Spark-native learned count/frequency encoding."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_categorical_columns,
    validate_column_presence,
    validate_column_types,
    validate_supported_option,
)

from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel

_SUPPORTED_METHODS = ("count", "frequency")
_SUPPORTED_UNSEEN_POLICIES = ("ignore", "encode", "raise")


def _normalize_method(method: str) -> str:
    return validate_supported_option("method", method, allowed=_SUPPORTED_METHODS)


def _normalize_unseen_policy(unseen: str) -> str:
    return validate_supported_option(
        "unseen",
        unseen,
        allowed=_SUPPORTED_UNSEEN_POLICIES,
    )


def _learn_mappings(
    dataset: DataFrame,
    variables: Sequence[str],
    method: str,
) -> dict[str, dict[str, int | float]]:
    total_rows = dataset.count() if method == "frequency" else None
    mappings: dict[str, dict[str, int | float]] = {}

    for variable in variables:
        rows = (
            dataset.where(F.col(variable).isNotNull())
            .groupBy(F.col(variable).alias("category"))
            .agg(F.count(F.lit(1)).alias("value"))
            .orderBy(F.col("category").asc())
            .collect()
        )

        if method == "count":
            mappings[variable] = {row.category: int(row.value) for row in rows}
            continue

        assert total_rows is not None
        denominator = float(total_rows)
        mappings[variable] = {
            row.category: (float(row.value) / denominator if denominator else 0.0)
            for row in rows
        }

    return mappings


def _mapping_expression(mapping: dict[str, int | float], method: str) -> Column:
    items: list[Column] = []
    for category, value in mapping.items():
        items.extend((F.lit(category), F.lit(value)))

    if items:
        return F.create_map(*items)

    value_type = "double" if method == "frequency" else "int"
    return F.create_map().cast(f"map<string,{value_type}>")


def _encoded_column(
    variable: str,
    mapping: dict[str, int | float],
    *,
    method: str,
    unseen: str,
) -> Column:
    mapped = _mapping_expression(mapping, method)[F.col(variable)]
    value_type = "double" if method == "frequency" else "int"
    encoded = mapped.cast(value_type)

    if unseen == "ignore":
        return encoded.alias(variable)

    if unseen == "encode":
        zero = F.lit(0.0) if method == "frequency" else F.lit(0)
        return (
            F.when(F.col(variable).isNull(), F.lit(None))
            .otherwise(F.coalesce(encoded, zero.cast(value_type)))
            .cast(value_type)
            .alias(variable)
        )

    return encoded.alias(variable)


def _raise_for_unseen_values(
    dataset: DataFrame,
    variables: Sequence[str],
    mappings: dict[str, dict[str, int | float]],
) -> None:
    for variable in variables:
        known_categories = list(mappings[variable])
        unseen_row = (
            dataset.where(F.col(variable).isNotNull())
            .where(~F.col(variable).isin(known_categories))
            .select(variable)
            .distinct()
            .first()
        )
        if unseen_row is not None:
            raise ValueError(
                f"Found unseen category for {variable}: {unseen_row[variable]!r}"
            )


class CountFrequencyEncoder(BaseSparkEstimator):
    """Learn deterministic category-to-count or category-to-frequency mappings."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        method: str = "count",
        unseen: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        self.method = _normalize_method(method)
        self.unseen = _normalize_unseen_policy(unseen)

    def _fit(self, dataset: DataFrame) -> "CountFrequencyEncoderModel":
        variables = resolve_categorical_columns(dataset, variables=self.get_variables())
        mappings = _learn_mappings(dataset, variables, self.method)

        return CountFrequencyEncoderModel(
            variables_=list(variables),
            mappings_=mappings,
            method_=self.method,
            unseen_=self.unseen,
        )


class CountFrequencyEncoderModel(BaseSparkModel):
    """Fitted count/frequency encoder backed by native Spark expressions."""

    variables_: list[str]
    mappings_: dict[str, dict[str, int | float]]
    method_: str
    unseen_: str

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        mappings_: dict[str, dict[str, int | float]],
        method_: str,
        unseen_: str,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute(
            "mappings_",
            {variable: dict(mapping) for variable, mapping in mappings_.items()},
        )
        self._set_learned_attribute("method_", method_)
        self._set_learned_attribute("unseen_", unseen_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "mappings_", "method_", "unseen_")
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="string")

        if self.unseen_ == "raise":
            _raise_for_unseen_values(dataset, self.variables_, self.mappings_)

        projections: list[Column] = []
        for column_name in dataset.columns:
            if column_name in self.variables_:
                projections.append(
                    _encoded_column(
                        column_name,
                        self.mappings_[column_name],
                        method=self.method_,
                        unseen=self.unseen_,
                    )
                )
            else:
                projections.append(F.col(column_name))

        return dataset.select(*projections)


__all__ = ("CountFrequencyEncoder", "CountFrequencyEncoderModel")
