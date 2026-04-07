"""Contract tests for the learned count/frequency encoder."""

from __future__ import annotations

import pytest

from spark_feature_engine.encoding.count_frequency import (
    CountFrequencyEncoder,
    CountFrequencyEncoderModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_count_mode_learns_category_occurrence_totals(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, "paris", "gold", 9.5, "keep-a"),
            (2, "london", "silver", 8.0, "keep-b"),
            (3, "paris", "gold", 7.0, "keep-c"),
            (4, "rome", "silver", 6.0, "keep-d"),
        ],
        schema="row_id INT, city STRING, tier STRING, score DOUBLE, label STRING",
    )

    model = CountFrequencyEncoder(variables=["city", "tier"], method="count").fit(
        dataset
    )
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, CountFrequencyEncoderModel)
    assert model.variables_ == ["city", "tier"]
    assert model.method_ == "count"
    assert model.mappings_ == {
        "city": {"london": 1, "paris": 2, "rome": 1},
        "tier": {"gold": 2, "silver": 2},
    }
    assert model.transform(dataset).columns == dataset.columns
    assert result[0].asDict() == {
        "row_id": 1,
        "city": 2,
        "tier": 2,
        "score": 9.5,
        "label": "keep-a",
    }
    assert result[1].asDict() == {
        "row_id": 2,
        "city": 1,
        "tier": 2,
        "score": 8.0,
        "label": "keep-b",
    }
    assert result[2].asDict() == {
        "row_id": 3,
        "city": 2,
        "tier": 2,
        "score": 7.0,
        "label": "keep-c",
    }
    assert result[3].asDict() == {
        "row_id": 4,
        "city": 1,
        "tier": 2,
        "score": 6.0,
        "label": "keep-d",
    }


def test_frequency_mode_learns_category_row_proportions(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "paris"), (4, "rome")],
        schema="row_id INT, city STRING",
    )

    model = CountFrequencyEncoder(variables=["city"], method="frequency").fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, CountFrequencyEncoderModel)
    assert model.method_ == "frequency"
    assert list(model.mappings_["city"].items()) == [
        ("london", 0.25),
        ("paris", 0.5),
        ("rome", 0.25),
    ]
    assert result[0].asDict() == {"row_id": 1, "city": pytest.approx(0.5)}
    assert result[1].asDict() == {"row_id": 2, "city": pytest.approx(0.25)}
    assert result[2].asDict() == {"row_id": 3, "city": pytest.approx(0.5)}
    assert result[3].asDict() == {"row_id": 4, "city": pytest.approx(0.25)}


def test_transform_with_ignore_policy_preserves_null_like_output_for_unseen_values(
    spark_session,
) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome"), (4, "london"), (5, None)], schema="row_id INT, city STRING"
    )

    model = CountFrequencyEncoder(
        variables=["city"], method="count", unseen="ignore"
    ).fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert result[0].asDict() == {"row_id": 3, "city": None}
    assert result[1].asDict() == {"row_id": 4, "city": 1}
    assert result[2].asDict() == {"row_id": 5, "city": None}


def test_transform_with_encode_policy_uses_zero_for_unseen_values(
    spark_session,
) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome"), (4, "paris")], schema="row_id INT, city STRING"
    )

    model = CountFrequencyEncoder(
        variables=["city"], method="frequency", unseen="encode"
    ).fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert model.mappings_ == {"city": {"london": 0.5, "paris": 0.5}}
    assert result[0].asDict() == {"row_id": 3, "city": pytest.approx(0.0)}
    assert result[1].asDict() == {"row_id": 4, "city": pytest.approx(0.5)}


def test_transform_with_raise_policy_fails_for_unseen_values(spark_session) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome")], schema="row_id INT, city STRING"
    )

    model = CountFrequencyEncoder(
        variables=["city"], method="count", unseen="raise"
    ).fit(train)

    with pytest.raises(ValueError, match="rome"):
        model.transform(future).collect()


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "east"), (2, "london", "west"), (3, "paris", "east")],
        schema="row_id INT, city STRING, region STRING",
    )

    model = CountFrequencyEncoder(
        variables=["city", "region"], method="count", unseen="ignore"
    ).fit(dataset)

    assert isinstance(model, CountFrequencyEncoderModel)
    assert model.variables_ == ["city", "region"]
    assert model.method_ == "count"
    assert model.unseen_ == "ignore"
    assert model.mappings_ == {
        "city": {"london": 1, "paris": 2},
        "region": {"east": 2, "west": 1},
    }
    assert not hasattr(model, "mappings")
    assert not hasattr(model, "method")
    assert not hasattr(model, "unseen")


def test_learned_mappings_are_deterministic_across_repeated_fits(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "berlin"), (4, "paris")],
        schema="row_id INT, city STRING",
    )

    first_model = CountFrequencyEncoder(variables=["city"], method="count").fit(dataset)
    second_model = CountFrequencyEncoder(variables=["city"], method="count").fit(
        dataset
    )

    assert list(first_model.mappings_["city"].items()) == [
        ("berlin", 1),
        ("london", 1),
        ("paris", 2),
    ]
    assert second_model.mappings_ == first_model.mappings_
    assert (
        first_model.transform(dataset).collect()
        == second_model.transform(dataset).collect()
    )


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 9.5, "north"), (2, "london", 8.0, "south")],
        schema="row_id INT, city STRING, score DOUBLE, segment STRING",
    )

    model = CountFrequencyEncoder(variables=["city"], method="count").fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.score for row in result] == [9.5, 8.0]
    assert [row.segment for row in result] == ["north", "south"]
    assert [row.city for row in result] == [1, 1]


def test_count_frequency_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "paris")],
        schema="row_id INT, city STRING",
    )

    model = CountFrequencyEncoder(variables=["city"], method="count").fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.mappings_ == {"city": {"london": 1, "paris": 2}}
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
