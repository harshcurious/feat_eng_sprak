"""Tests for the Adult Census download/example helpers."""

from __future__ import annotations

from pathlib import Path

from examples.download_adult_census import (
    canonicalize_column_name,
    create_raw_assets,
    load_adult_census_frame,
)


def test_canonicalize_column_name_normalizes_symbols_and_spacing() -> None:
    assert canonicalize_column_name(" hours-per-week ") == "hours_per_week"
    assert canonicalize_column_name("education.num") == "education_num"


def test_create_raw_assets_creates_managed_table_and_temp_view(
    spark_session,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "adult.csv"
    csv_path.write_text(
        "age,education.num,hours-per-week,income\n39,13,40,<=50K\n50,13,13,>50K\n",
        encoding="utf-8",
    )

    dataset = load_adult_census_frame(spark_session, csv_path)
    table_name = "adult_census_example_raw_test"
    view_name = "adult_census_example_raw_view_test"

    spark_session.sql(f"DROP VIEW IF EXISTS {view_name}")
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")

    try:
        create_raw_assets(
            spark_session,
            dataset,
            table_name=table_name,
            view_name=view_name,
        )

        assert spark_session.catalog.tableExists(table_name)
        assert spark_session.catalog.tableExists(view_name)
        assert dataset.columns == ["age", "education_num", "hours_per_week", "income"]
        assert spark_session.table(table_name).count() == 2
        assert spark_session.table(view_name).count() == 2
    finally:
        spark_session.sql(f"DROP VIEW IF EXISTS {view_name}")
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
