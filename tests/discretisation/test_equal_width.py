"""Contract tests for the learned equal-width discretiser."""

from __future__ import annotations

import math

import pytest

from spark_feature_engine.discretisation.equal_width import (
    EqualWidthDiscretiser,
    EqualWidthDiscretiserModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def _boundary_label(lower: float, upper: float) -> str:
    lower_text = "-inf" if math.isinf(lower) and lower < 0 else f"{lower:.1f}"
    upper_text = "inf" if math.isinf(upper) and upper > 0 else f"{upper:.1f}"
    return f"({lower_text}, {upper_text}]"


def test_fit_learns_equal_width_boundaries_for_each_selected_variable(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0.0, 10.0, "keep-a"),
            (2, 1.0, 11.0, "keep-b"),
            (3, 3.0, 13.0, "keep-c"),
            (4, 5.0, 15.0, "keep-d"),
            (5, 8.0, 18.0, "keep-e"),
        ],
        schema="row_id INT, score DOUBLE, visits DOUBLE, label STRING",
    )

    model = EqualWidthDiscretiser(variables=["score", "visits"], bin_count=4).fit(
        dataset
    )

    assert isinstance(model, EqualWidthDiscretiserModel)
    assert model.boundaries_ == {
        "score": [float("-inf"), 2.0, 4.0, 6.0, float("inf")],
        "visits": [float("-inf"), 12.0, 14.0, 16.0, float("inf")],
    }


def test_transform_replaces_selected_columns_with_ordered_bin_assignments(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (1, 0.0, 10.0, "keep-a"),
            (2, 1.0, 11.0, "keep-b"),
            (3, 3.0, 13.0, "keep-c"),
            (4, 5.0, 15.0, "keep-d"),
            (5, 8.0, 18.0, "keep-e"),
        ],
        schema="row_id INT, score DOUBLE, visits DOUBLE, label STRING",
    )

    model = EqualWidthDiscretiser(variables=["score", "visits"], bin_count=4).fit(
        dataset
    )
    result = model.transform(dataset).orderBy("row_id").collect()

    assert model.transform(dataset).columns == dataset.columns
    assert result[0].asDict() == {
        "row_id": 1,
        "score": 0,
        "visits": 0,
        "label": "keep-a",
    }
    assert result[1].asDict() == {
        "row_id": 2,
        "score": 0,
        "visits": 0,
        "label": "keep-b",
    }
    assert result[2].asDict() == {
        "row_id": 3,
        "score": 1,
        "visits": 1,
        "label": "keep-c",
    }
    assert result[3].asDict() == {
        "row_id": 4,
        "score": 2,
        "visits": 2,
        "label": "keep-d",
    }
    assert result[4].asDict() == {
        "row_id": 5,
        "score": 3,
        "visits": 3,
        "label": "keep-e",
    }


def test_transform_clips_future_extremes_into_outer_bins(spark_session) -> None:
    train = spark_session.createDataFrame(
        [(1, 0.0), (2, 2.0), (3, 4.0), (4, 8.0)],
        schema="row_id INT, score DOUBLE",
    )
    future = spark_session.createDataFrame(
        [(5, -100.0), (6, 1.0), (7, 100.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = EqualWidthDiscretiser(variables=["score"], bin_count=4).fit(train)
    result = model.transform(future).orderBy("row_id").collect()

    assert result[0].asDict() == {"row_id": 5, "score": 0}
    assert result[1].asDict() == {"row_id": 6, "score": 0}
    assert result[2].asDict() == {"row_id": 7, "score": 3}


def test_transform_supports_numeric_bins_and_boundary_label_outputs(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 3.0), (3, 5.0), (4, 8.0)],
        schema="row_id INT, score DOUBLE",
    )

    numeric_model = EqualWidthDiscretiser(variables=["score"], bin_count=4).fit(dataset)
    labelled_model = EqualWidthDiscretiser(
        variables=["score"], bin_count=4, output="boundaries"
    ).fit(dataset)

    numeric_result = numeric_model.transform(dataset).orderBy("row_id").collect()
    labelled_result = labelled_model.transform(dataset).orderBy("row_id").collect()

    assert [row.score for row in numeric_result] == [0, 1, 2, 3]
    assert labelled_model.boundaries_ == {
        "score": [float("-inf"), 2.0, 4.0, 6.0, float("inf")]
    }
    assert [row.score for row in labelled_result] == [
        _boundary_label(float("-inf"), 2.0),
        _boundary_label(2.0, 4.0),
        _boundary_label(4.0, 6.0),
        _boundary_label(6.0, float("inf")),
    ]


def test_fitted_model_exposes_learned_trailing_underscore_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 2.0), (3, 4.0), (4, 8.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = EqualWidthDiscretiser(
        variables=["score"], bin_count=4, output="boundaries"
    ).fit(dataset)

    assert isinstance(model, EqualWidthDiscretiserModel)
    assert model.variables_ == ["score"]
    assert model.bin_count_ == 4
    assert model.output_ == "boundaries"
    assert model.boundaries_ == {"score": [float("-inf"), 2.0, 4.0, 6.0, float("inf")]}
    assert not hasattr(model, "bin_count")
    assert not hasattr(model, "output")
    assert not hasattr(model, "boundaries")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"variables": ["missing"], "bin_count": 4}, "missing"),
        ({"variables": ["city"], "bin_count": 4}, "numeric"),
        ({"variables": ["score", "score"], "bin_count": 4}, "Duplicate"),
        ({"variables": ["score"], "bin_count": 1}, "bin_count"),
        ({"variables": ["score"], "bin_count": 4, "output": "labels"}, "labels"),
    ],
)
def test_fit_rejects_invalid_configuration_and_targets(
    spark_session,
    kwargs: dict[str, object],
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0, "paris")], schema="row_id INT, score DOUBLE, city STRING"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        EqualWidthDiscretiser(**kwargs).fit(dataset)


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0, "north", 9.5), (2, 8.0, "south", 8.0)],
        schema="row_id INT, score DOUBLE, segment STRING, raw_value DOUBLE",
    )

    model = EqualWidthDiscretiser(variables=["score"], bin_count=4).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.segment for row in result] == ["north", "south"]
    assert [row.raw_value for row in result] == [9.5, 8.0]
    assert [row.score for row in result] == [0, 3]


def test_equal_width_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 2.0), (3, 4.0), (4, 8.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = EqualWidthDiscretiser(variables=["score"], bin_count=4).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert model.boundaries_ == {"score": [float("-inf"), 2.0, 4.0, 6.0, float("inf")]}
    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
