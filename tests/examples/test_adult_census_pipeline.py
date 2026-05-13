"""Tests for the Adult Census modeling example helpers."""

from __future__ import annotations

from pathlib import Path

import examples.adult_census_pipeline as adult_census_pipeline
import pytest
from examples.adult_census_pipeline import (
    apply_education_ordinal_encoding,
    apply_native_country_binary_encoding,
    assert_improvement,
    ensure_raw_table,
    numeric_and_categorical_features,
    score_model_frame,
    with_income_label,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_with_income_label_creates_binary_numeric_target(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(" <=50K", 39), (" >50K.", 50)],
        schema="income STRING, age INT",
    )

    result = with_income_label(dataset).orderBy("age").collect()

    assert result[0].income_label == 0
    assert result[1].income_label == 1


def test_numeric_and_categorical_features_excludes_target_column(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(39, 13, " Private", 0)],
        schema="age INT, education_num INT, workclass STRING, income_label INT",
    )

    numeric_features, categorical_features = numeric_and_categorical_features(
        with_income_label(
            dataset, source_column="income_label", output_column="label_copy"
        ),
        label_column="label_copy",
    )

    assert numeric_features == ["age", "education_num", "income_label"]
    assert categorical_features == ["workclass"]


def test_assert_improvement_rejects_non_improving_scores() -> None:
    with pytest.raises(RuntimeError, match="did not beat"):
        assert_improvement(baseline_score=0.88, improved_score=0.88)


def test_score_model_frame_returns_float_metric(spark_session) -> None:
    train = spark_session.createDataFrame(
        [
            (1.0, 0.0, 0),
            (2.0, 0.0, 0),
            (8.0, 1.0, 1),
            (9.0, 1.0, 1),
        ],
        schema="feature_a DOUBLE, feature_b DOUBLE, income_label INT",
    )
    test = spark_session.createDataFrame(
        [(1.5, 0.0, 0), (8.5, 1.0, 1)],
        schema="feature_a DOUBLE, feature_b DOUBLE, income_label INT",
    )

    score = score_model_frame(train, test, label_column="income_label")

    assert isinstance(score, float)
    assert 0.0 <= score <= 1.0


def test_apply_education_ordinal_encoding_uses_semantic_order(spark_session) -> None:
    train = spark_session.createDataFrame(
        [("12th",), ("Bachelors",), ("Masters",), ("HS-grad",)],
        schema="education STRING",
    )
    test = spark_session.createDataFrame(
        [("Doctorate",), ("Some-college",)],
        schema="education STRING",
    )

    encoded_train, encoded_test = apply_education_ordinal_encoding(train, test)
    train_rows = {row.education for row in encoded_train.collect()}
    test_rows = {row.education for row in encoded_test.collect()}

    assert min(train_rows) == 7
    assert 7 in train_rows
    assert 12 in train_rows
    assert 13 in train_rows
    assert 15 in test_rows
    assert 9 in test_rows


def test_apply_native_country_binary_encoding_maps_united_states(spark_session) -> None:
    train = spark_session.createDataFrame(
        [("United-States",), ("Mexico",), (None,)],
        schema="native_country STRING",
    )
    test = spark_session.createDataFrame(
        [("United-States",), ("Canada",)],
        schema="native_country STRING",
    )

    encoded_train, encoded_test = apply_native_country_binary_encoding(train, test)

    assert [row.native_country for row in encoded_train.collect()] == [1, 0, None]
    assert [row.native_country for row in encoded_test.collect()] == [1, 0]


def test_marimo_app_is_exported() -> None:
    assert adult_census_pipeline.app.__class__.__name__ == "App"


def test_force_download_refreshes_existing_raw_table(monkeypatch) -> None:
    calls: list[str] = []

    class _FakeRegisteredTable:
        def createOrReplaceTempView(self, name: str) -> None:
            calls.append(f"view:{name}")

    class _FakeCatalog:
        def tableExists(self, table_name: str) -> bool:
            assert table_name == adult_census_pipeline.RAW_TABLE_NAME
            return True

    class _FakeSpark:
        catalog = _FakeCatalog()

        def table(self, table_name: str) -> _FakeRegisteredTable:
            calls.append(f"table:{table_name}")
            return _FakeRegisteredTable()

    monkeypatch.setattr(
        adult_census_pipeline,
        "download_dataset",
        lambda **_: calls.append("download") or Path("/tmp/adult_census"),
    )
    monkeypatch.setattr(
        adult_census_pipeline,
        "load_extracted_dataset",
        lambda *_args, **_kwargs: ("dataset", Path("/tmp/adult_census/adult.csv")),
    )
    monkeypatch.setattr(
        adult_census_pipeline,
        "create_raw_assets",
        lambda *_args, **_kwargs: calls.append("create"),
    )

    ensure_raw_table(_FakeSpark(), force_download=True)

    assert "download" in calls
    assert "create" in calls
