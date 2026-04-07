"""Spark-native learned ordinal encoding."""

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

_SUPPORTED_UNSEEN_POLICIES = ("ignore", "encode", "raise")
_RESERVED_UNSEEN_CODE = -1


def _normalize_unseen_policy(unseen: str) -> str:
    return validate_supported_option(
        "unseen",
        unseen,
        allowed=_SUPPORTED_UNSEEN_POLICIES,
    )


def _learn_mappings(
    dataset: DataFrame, variables: Sequence[str]
) -> dict[str, dict[str, int]]:
    mappings: dict[str, dict[str, int]] = {}
    for variable in variables:
        rows = (
            dataset.where(F.col(variable).isNotNull())
            .select(F.col(variable).alias("category"))
            .distinct()
            .orderBy(F.col("category").asc())
            .collect()
        )
        mappings[variable] = {row.category: code for code, row in enumerate(rows)}
    return mappings


def _mapping_expression(mapping: dict[str, int]) -> Column:
    items: list[Column] = []
    for category, code in mapping.items():
        items.extend((F.lit(category), F.lit(code)))
    if not items:
        return F.create_map().cast("map<string,int>")
    return F.create_map(*items)


def _encoded_column(
    variable: str,
    mapping: dict[str, int],
    unseen: str,
) -> Column:
    mapped = _mapping_expression(mapping)[F.col(variable)]
    if unseen == "ignore":
        return mapped.cast("int").alias(variable)
    if unseen == "encode":
        return (
            F.when(F.col(variable).isNull(), F.lit(None))
            .otherwise(F.coalesce(mapped, F.lit(_RESERVED_UNSEEN_CODE)))
            .cast("int")
            .alias(variable)
        )
    return mapped.cast("int").alias(variable)


def _raise_for_unseen_values(
    dataset: DataFrame,
    variables: Sequence[str],
    mappings: dict[str, dict[str, int]],
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


class OrdinalEncoder(BaseSparkEstimator):
    """Learn deterministic category-to-integer mappings."""

    def __init__(
        self,
        *,
        variables: Sequence[str] | None = None,
        unseen: str = "ignore",
    ) -> None:
        super().__init__(variables=variables)
        self.unseen = _normalize_unseen_policy(unseen)

    def _fit(self, dataset: DataFrame) -> "OrdinalEncoderModel":
        variables = resolve_categorical_columns(dataset, variables=self.get_variables())
        mappings = _learn_mappings(dataset, variables)

        return OrdinalEncoderModel(
            variables_=list(variables),
            mappings_=mappings,
            unseen_=self.unseen,
        )


class OrdinalEncoderModel(BaseSparkModel):
    """Fitted ordinal encoder backed by native Spark expressions."""

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        mappings_: dict[str, dict[str, int]],
        unseen_: str,
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute(
            "mappings_",
            {variable: dict(mapping) for variable, mapping in mappings_.items()},
        )
        self._set_learned_attribute("unseen_", unseen_)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "mappings_", "unseen_")
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
                        self.unseen_,
                    )
                )
            else:
                projections.append(F.col(column_name))

        return dataset.select(*projections)


__all__ = ("OrdinalEncoder", "OrdinalEncoderModel")
