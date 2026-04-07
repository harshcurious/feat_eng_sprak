"""Spark-native learned one-hot encoding."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_feature_engine._validation import (
    resolve_categorical_columns,
    validate_column_presence,
    validate_column_types,
    validate_generated_column_names,
)
from spark_feature_engine.base import BaseSparkEstimator, BaseSparkModel


def _generated_column_name(variable: str, category: str) -> str:
    return f"{variable}_{category}"


def _learn_categories(
    dataset: DataFrame, variables: Sequence[str]
) -> dict[str, list[str]]:
    categories: dict[str, list[str]] = {}
    for variable in variables:
        rows = (
            dataset.where(F.col(variable).isNotNull())
            .select(F.col(variable).alias("category"))
            .distinct()
            .orderBy(F.col("category").asc())
            .collect()
        )
        categories[variable] = [row.category for row in rows]
    return categories


def _build_generated_columns(
    categories: dict[str, list[str]],
) -> dict[str, list[str]]:
    return {
        variable: [_generated_column_name(variable, category) for category in learned]
        for variable, learned in categories.items()
    }


def _validate_output_columns(
    dataset: DataFrame,
    variables: Sequence[str],
    generated_columns: dict[str, list[str]],
) -> None:
    flat_generated = [
        column for names in generated_columns.values() for column in names
    ]
    validate_generated_column_names(
        dataset,
        flat_generated,
        ignore_existing=variables,
    )


def _one_hot_expression(variable: str, category: str, output_name: str) -> Column:
    return (
        F.when(F.col(variable) == F.lit(category), F.lit(1))
        .otherwise(F.lit(0))
        .cast("int")
        .alias(output_name)
    )


class OneHotEncoder(BaseSparkEstimator):
    """Learn categorical values and replace source columns with binary columns."""

    def __init__(self, *, variables: Sequence[str] | None = None) -> None:
        super().__init__(variables=variables)

    def _fit(self, dataset: DataFrame) -> "OneHotEncoderModel":
        variables = resolve_categorical_columns(dataset, variables=self.get_variables())
        categories = _learn_categories(dataset, variables)
        generated_columns = _build_generated_columns(categories)
        _validate_output_columns(dataset, variables, generated_columns)

        return OneHotEncoderModel(
            variables_=list(variables),
            categories_=categories,
            generated_columns_=generated_columns,
        )


class OneHotEncoderModel(BaseSparkModel):
    """Fitted one-hot encoder backed by native Spark expressions."""

    def __init__(
        self,
        *,
        variables_: Sequence[str],
        categories_: dict[str, list[str]],
        generated_columns_: dict[str, list[str]],
    ) -> None:
        super().__init__()
        self._set_learned_attribute("variables_", list(variables_))
        self._set_learned_attribute(
            "categories_",
            {
                variable: list(categories)
                for variable, categories in categories_.items()
            },
        )
        self._set_learned_attribute(
            "generated_columns_",
            {
                variable: list(columns)
                for variable, columns in generated_columns_.items()
            },
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        self.require_fitted("variables_", "categories_", "generated_columns_")
        validate_column_presence(dataset, self.variables_)
        validate_column_types(dataset, self.variables_, expected_type="string")
        _validate_output_columns(dataset, self.variables_, self.generated_columns_)

        projections: list[Column] = []
        for column_name in dataset.columns:
            if column_name not in self.variables_:
                projections.append(F.col(column_name))
                continue

            for category, output_name in zip(
                self.categories_[column_name],
                self.generated_columns_[column_name],
            ):
                projections.append(
                    _one_hot_expression(column_name, category, output_name)
                )

        return dataset.select(*projections)


__all__ = ("OneHotEncoder", "OneHotEncoderModel")
