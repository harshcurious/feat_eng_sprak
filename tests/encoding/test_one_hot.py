"""Contract tests for the learned one-hot encoder."""

from __future__ import annotations

import pytest

from spark_feature_engine.encoding.one_hot import OneHotEncoder, OneHotEncoderModel


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_fit_learns_categories_and_transform_expands_binary_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, "paris", "east", "keep-a"),
            (2, "london", "west", "keep-b"),
            (3, "paris", "east", "keep-c"),
        ],
        schema="row_id INT, city STRING, region STRING, label STRING",
    )

    model = OneHotEncoder(variables=["city", "region"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert isinstance(model, OneHotEncoderModel)
    assert model.categories_ == {
        "city": ["london", "paris"],
        "region": ["east", "west"],
    }
    assert result[0].asDict() == {
        "row_id": 1,
        "city_london": 0,
        "city_paris": 1,
        "region_east": 1,
        "region_west": 0,
        "label": "keep-a",
    }
    assert result[1].asDict() == {
        "row_id": 2,
        "city_london": 1,
        "city_paris": 0,
        "region_east": 0,
        "region_west": 1,
        "label": "keep-b",
    }
    assert result[2].asDict() == {
        "row_id": 3,
        "city_london": 0,
        "city_paris": 1,
        "region_east": 1,
        "region_west": 0,
        "label": "keep-c",
    }
    assert model.transform(dataset).columns == [
        "row_id",
        "city_london",
        "city_paris",
        "region_east",
        "region_west",
        "label",
    ]


def test_generated_column_names_are_deterministic_across_repeated_fits(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "berlin"), (4, "paris")],
        schema="row_id INT, city STRING",
    )

    first_model = OneHotEncoder(variables=["city"]).fit(dataset)
    second_model = OneHotEncoder(variables=["city"]).fit(dataset)

    assert first_model.categories_ == {"city": ["berlin", "london", "paris"]}
    assert first_model.generated_columns_ == {
        "city": ["city_berlin", "city_london", "city_paris"]
    }
    assert second_model.categories_ == first_model.categories_
    assert second_model.generated_columns_ == first_model.generated_columns_
    assert (
        first_model.transform(dataset).columns
        == second_model.transform(dataset).columns
    )


def test_fit_rejects_generated_name_collision_with_existing_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "london", 1), (2, "paris", 0)],
        schema="row_id INT, city STRING, city_london INT",
    )

    with pytest.raises(ValueError, match="city_london"):
        OneHotEncoder(variables=["city"]).fit(dataset)


def test_fit_rejects_generated_name_collision_between_selected_variables(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "red_blue", "blue"), (2, "green", "yellow")],
        schema="row_id INT, color STRING, color_red STRING",
    )

    with pytest.raises(ValueError, match="color_red_blue"):
        OneHotEncoder(variables=["color", "color_red"]).fit(dataset)


def test_transform_leaves_unseen_categories_as_all_zero_rows(spark_session) -> None:
    train = spark_session.createDataFrame(
        [(1, "london"), (2, "paris")], schema="row_id INT, city STRING"
    )
    future = spark_session.createDataFrame(
        [(3, "rome"), (4, "london")], schema="row_id INT, city STRING"
    )

    model = OneHotEncoder(variables=["city"]).fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert result[0].asDict() == {"row_id": 3, "city_london": 0, "city_paris": 0}
    assert result[1].asDict() == {"row_id": 4, "city_london": 1, "city_paris": 0}


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", 9.5, "north"), (2, "london", 8.0, "south")],
        schema="row_id INT, city STRING, score DOUBLE, segment STRING",
    )

    model = OneHotEncoder(variables=["city"]).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.score for row in result] == [9.5, 8.0]
    assert [row.segment for row in result] == ["north", "south"]
    assert "city" not in model.transform(dataset).columns


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris", "east"), (2, "london", "west")],
        schema="row_id INT, city STRING, region STRING",
    )

    model = OneHotEncoder(variables=["city", "region"]).fit(dataset)

    assert isinstance(model, OneHotEncoderModel)
    assert model.variables_ == ["city", "region"]
    assert model.categories_ == {
        "city": ["london", "paris"],
        "region": ["east", "west"],
    }
    assert model.generated_columns_ == {
        "city": ["city_london", "city_paris"],
        "region": ["region_east", "region_west"],
    }
    assert not hasattr(model, "categories")
    assert not hasattr(model, "generated_columns")


def test_one_hot_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, "paris"), (2, "london"), (3, "paris")],
        schema="row_id INT, city STRING",
    )

    model = OneHotEncoder(variables=["city"]).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.generated_columns_ == {"city": ["city_london", "city_paris"]}
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
