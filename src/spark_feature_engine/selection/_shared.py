"""Internal shared helpers for advanced selection."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def assign_deterministic_folds(
    dataset: DataFrame,
    *,
    n_splits: int,
    seed: int,
    order_by: list[str],
    fold_col: str = "_fold_id",
) -> DataFrame:
    """Assign deterministic fold ids using stable Spark expressions only."""
    if not isinstance(n_splits, int) or isinstance(n_splits, bool) or n_splits < 2:
        raise ValueError("n_splits must be an integer greater than or equal to 2")

    hashed_inputs = [F.lit(seed)] + [
        F.col(column).cast("string") for column in order_by
    ]
    ordering = Window.orderBy(
        F.xxhash64(*hashed_inputs),
        *[F.col(column) for column in order_by],
    )
    return dataset.withColumn(
        fold_col,
        F.pmod(
            F.row_number().over(ordering) - F.lit(1) + F.lit(seed), F.lit(n_splits)
        ).cast("int"),
    )


__all__ = ("assign_deterministic_folds",)
