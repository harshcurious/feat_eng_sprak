"""Contract tests for the learned rare-label encoder."""

from __future__ import annotations

import pytest

from spark_feature_engine.encoding.rare_label import (
    RareLabelEncoder,
    RareLabelEncoderModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_frequency_threshold_groups_infrequent_categories(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, "paris", "gold", 9.5, "keep-a"),
            (2, "paris", "gold", 8.0, "keep-b"),
            (3, "paris", "silver", 7.0, "keep-c"),
            (4, "london", "gold", 6.0, "keep-d"),
            (5, "london", "silver", 5.0, "keep-e"),
            (6, "rome", "bronze", 4.0, "keep-f"),
        ],
        schema="row_id INT, city STRING, tier STRING, score DOUBLE, label STRING",
    )

    model = RareLabelEncoder(
        variables=["city", "tier"],
        tolerance=0.30,
        min_categories=3,
        replacement_label="Rare",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, RareLabelEncoderModel)
    assert model.variables_ == ["city", "tier"]
    assert model.frequent_labels_ == {
        "city": ["london", "paris"],
        "tier": ["gold", "silver"],
    }
    assert model.transform(dataset).columns == dataset.columns
    assert result[0].asDict() == {
        "row_id": 1,
        "city": "paris",
        "tier": "gold",
        "score": 9.5,
        "label": "keep-a",
    }
    assert result[4].asDict() == {
        "row_id": 5,
        "city": "london",
        "tier": "silver",
        "score": 5.0,
        "label": "keep-e",
    }
    assert result[5].asDict() == {
        "row_id": 6,
        "city": "Rare",
        "tier": "Rare",
        "score": 4.0,
        "label": "keep-f",
    }


def test_minimum_category_threshold_bypasses_grouping_for_low_cardinality_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "a"), (2, "a"), (3, "a"), (4, "b")],
        schema="row_id INT, city STRING",
    )

    model = RareLabelEncoder(
        variables=["city"],
        tolerance=0.80,
        min_categories=2,
        replacement_label="Rare",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.frequent_labels_ == {"city": ["a", "b"]}
    assert [row.city for row in result] == ["a", "a", "a", "b"]


def test_maximum_frequent_category_cap_keeps_only_top_ranked_categories(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, "paris"),
            (2, "paris"),
            (3, "paris"),
            (4, "berlin"),
            (5, "berlin"),
            (6, "london"),
            (7, "london"),
            (8, "rome"),
        ],
        schema="row_id INT, city STRING",
    )

    model = RareLabelEncoder(
        variables=["city"],
        tolerance=0.10,
        min_categories=1,
        max_categories=2,
        replacement_label="Rare",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.frequent_labels_ == {"city": ["paris", "berlin"]}
    assert [row.city for row in result] == [
        "paris",
        "paris",
        "paris",
        "berlin",
        "berlin",
        "Rare",
        "Rare",
        "Rare",
    ]


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "east"), (2, "paris", "west"), (3, "london", "east")],
        schema="row_id INT, city STRING, region STRING",
    )

    model = RareLabelEncoder(
        variables=["city", "region"],
        tolerance=0.30,
        min_categories=2,
        max_categories=1,
        replacement_label="Other",
    ).fit(dataset)

    assert isinstance(model, RareLabelEncoderModel)
    assert model.variables_ == ["city", "region"]
    assert model.tolerance_ == 0.30
    assert model.min_categories_ == 2
    assert model.max_categories_ == 1
    assert model.replacement_label_ == "Other"
    assert model.frequent_labels_ == {
        "city": ["paris"],
        "region": ["east"],
    }
    assert not hasattr(model, "frequent_labels")
    assert not hasattr(model, "replacement_label")


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 9.5, "north"), (2, "rome", 8.0, "south")],
        schema="row_id INT, city STRING, score DOUBLE, segment STRING",
    )

    model = RareLabelEncoder(
        variables=["city"],
        tolerance=0.60,
        min_categories=1,
        replacement_label="Rare",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.score for row in result] == [9.5, 8.0]
    assert [row.segment for row in result] == ["north", "south"]
    assert [row.city for row in result] == ["Rare", "Rare"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"variables": ["missing"], "tolerance": 0.20}, "missing"),
        ({"variables": ["score"], "tolerance": 0.20}, "string"),
        ({"variables": ["city", "city"], "tolerance": 0.20}, "Duplicate"),
        ({"variables": ["city"], "tolerance": 0.0}, "tolerance"),
        ({"variables": ["city"], "tolerance": 1.5}, "tolerance"),
        ({"variables": ["city"], "tolerance": 0.20, "min_categories": 0}, "min"),
        ({"variables": ["city"], "tolerance": 0.20, "max_categories": 0}, "max"),
    ],
)
def test_fit_rejects_invalid_configuration_and_targets(
    spark_session,
    kwargs: dict[str, object],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 9.5)], schema="row_id INT, city STRING, score DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        RareLabelEncoder(replacement_label="Rare", **kwargs).fit(dataset)


def test_rare_label_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "paris"), (3, "london"), (4, "rome")],
        schema="row_id INT, city STRING",
    )

    model = RareLabelEncoder(
        variables=["city"],
        tolerance=0.30,
        min_categories=3,
        replacement_label="Rare",
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.frequent_labels_ == {"city": ["paris"]}
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
