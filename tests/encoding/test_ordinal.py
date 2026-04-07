"""Contract tests for the learned ordinal encoder."""

from __future__ import annotations

import pytest

from spark_feature_engine.encoding.ordinal import OrdinalEncoder, OrdinalEncoderModel


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_fit_learns_deterministic_mappings_and_replaces_selected_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, "paris", "b", 9.5, "keep-a"),
            (2, "london", "a", 8.0, "keep-b"),
            (3, "paris", "c", 7.0, "keep-c"),
        ],
        schema="row_id INT, city STRING, grade STRING, score DOUBLE, label STRING",
    )

    model = OrdinalEncoder(variables=["city", "grade"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, OrdinalEncoderModel)
    assert model.variables_ == ["city", "grade"]
    assert model.mappings_ == {
        "city": {"london": 0, "paris": 1},
        "grade": {"a": 0, "b": 1, "c": 2},
    }
    assert model.transform(dataset).columns == dataset.columns
    assert result[0].asDict() == {
        "row_id": 1,
        "city": 1,
        "grade": 1,
        "score": 9.5,
        "label": "keep-a",
    }
    assert result[1].asDict() == {
        "row_id": 2,
        "city": 0,
        "grade": 0,
        "score": 8.0,
        "label": "keep-b",
    }
    assert result[2].asDict() == {
        "row_id": 3,
        "city": 1,
        "grade": 2,
        "score": 7.0,
        "label": "keep-c",
    }


def test_learned_mappings_are_deterministic_across_repeated_fits(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "berlin"), (4, "paris")],
        schema="row_id INT, city STRING",
    )

    first_model = OrdinalEncoder(variables=["city"]).fit(dataset)
    second_model = OrdinalEncoder(variables=["city"]).fit(dataset)

    assert first_model.mappings_ == {"city": {"berlin": 0, "london": 1, "paris": 2}}
    assert second_model.mappings_ == first_model.mappings_
    assert (
        first_model.transform(dataset).collect()
        == second_model.transform(dataset).collect()
    )


def test_transform_with_ignore_policy_preserves_null_like_output_for_unseen_values(
    spark_session,
) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome"), (4, "london"), (5, None)], schema="row_id INT, city STRING"
    )

    model = OrdinalEncoder(variables=["city"], unseen="ignore").fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert result[0].asDict() == {"row_id": 3, "city": None}
    assert result[1].asDict() == {"row_id": 4, "city": 0}
    assert result[2].asDict() == {"row_id": 5, "city": None}


def test_transform_with_encode_policy_uses_reserved_unseen_code(spark_session) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome"), (4, "paris")], schema="row_id INT, city STRING"
    )

    model = OrdinalEncoder(variables=["city"], unseen="encode").fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert model.mappings_ == {"city": {"london": 0, "paris": 1}}
    assert result[0].asDict() == {"row_id": 3, "city": -1}
    assert result[1].asDict() == {"row_id": 4, "city": 1}


def test_transform_with_raise_policy_fails_for_unseen_values(spark_session) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome")], schema="row_id INT, city STRING"
    )

    model = OrdinalEncoder(variables=["city"], unseen="raise").fit(train)

    with pytest.raises(ValueError, match="rome"):
        model.transform(future).collect()


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "east"), (2, "london", "west")],
        schema="row_id INT, city STRING, region STRING",
    )

    model = OrdinalEncoder(variables=["city", "region"]).fit(dataset)

    assert isinstance(model, OrdinalEncoderModel)
    assert model.variables_ == ["city", "region"]
    assert model.mappings_ == {
        "city": {"london": 0, "paris": 1},
        "region": {"east": 0, "west": 1},
    }
    assert not hasattr(model, "mappings")


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 9.5, "north"), (2, "london", 8.0, "south")],
        schema="row_id INT, city STRING, score DOUBLE, segment STRING",
    )

    model = OrdinalEncoder(variables=["city"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.score for row in result] == [9.5, 8.0]
    assert [row.segment for row in result] == ["north", "south"]
    assert [row.city for row in result] == [1, 0]


def test_ordinal_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "paris")],
        schema="row_id INT, city STRING",
    )

    model = OrdinalEncoder(variables=["city"]).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.mappings_ == {"city": {"london": 0, "paris": 1}}
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
