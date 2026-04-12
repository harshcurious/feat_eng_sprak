"""Contract tests for the arbitrary discretiser."""

from __future__ import annotations

import math

import pytest

from spark_feature_engine.discretisation.arbitrary import (
    ArbitraryDiscretiser,
    ArbitraryDiscretiserModel,
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


def test_transform_uses_user_supplied_boundaries_for_assignment(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, -1.0), (2, 0.0), (3, 1.0), (4, 2.0), (5, 3.0), (6, 9.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0, 8.0]}
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.score for row in result] == [None, 0, 0, 1, 1, 2]


def test_transform_uses_left_open_right_closed_interval_semantics(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 1.0), (3, 2.0), (4, 4.0), (5, 8.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0, 8.0]}
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.score for row in result] == [0, 0, 1, 2, 3]


def test_transform_supports_ignore_policy_for_out_of_range_values(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, -10.0), (2, 1.0), (3, 10.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"],
        boundaries={"score": [0.0, 2.0, 4.0]},
        out_of_range="ignore",
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.score for row in result] == [None, 0, None]


def test_transform_rejects_out_of_range_values_when_policy_raises(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, -10.0), (2, 1.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"],
        boundaries={"score": [0.0, 2.0, 4.0]},
        out_of_range="raise",
    ).fit(dataset)

    with pytest.raises((TypeError, ValueError), match="out_of_range|range"):
        model.transform(dataset).collect()


def test_transform_supports_numeric_bins_and_boundary_label_outputs(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 1.0), (3, 2.0), (4, 4.0), (5, 8.0)],
        schema="row_id INT, score DOUBLE",
    )

    numeric_model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0, 8.0]}
    ).fit(dataset)
    labelled_model = ArbitraryDiscretiser(
        variables=["score"],
        boundaries={"score": [0.0, 2.0, 4.0, 8.0]},
        output="boundaries",
    ).fit(dataset)

    numeric_result = numeric_model.transform(dataset).orderBy("row_id").collect()
    labelled_result = labelled_model.transform(dataset).orderBy("row_id").collect()

    assert [row.score for row in numeric_result] == [0, 0, 1, 2, 3]
    assert [row.score for row in labelled_result] == [
        _boundary_label(0.0, 2.0),
        _boundary_label(0.0, 2.0),
        _boundary_label(2.0, 4.0),
        _boundary_label(4.0, 8.0),
        _boundary_label(8.0, float("inf")),
    ]


def test_fitted_model_remains_stateless_and_exposes_configured_attributes(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 2.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0]}, output="boundaries"
    ).fit(dataset)

    assert isinstance(model, ArbitraryDiscretiserModel)
    assert model.variables_ == ["score"]
    assert model.output_ == "boundaries"
    assert model.boundaries_ == {"score": [0.0, 2.0, 4.0]}
    assert not hasattr(model, "boundaries")
    assert not hasattr(model, "output")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"variables": ["missing"], "boundaries": {"missing": [0.0, 1.0]}}, "missing"),
        (
            {"variables": ["score", "score"], "boundaries": {"score": [0.0, 1.0]}},
            "Duplicate",
        ),
        ({"variables": ["score"], "boundaries": {"score": [1.0]}}, "boundary"),
        ({"variables": ["score"], "boundaries": {"score": [2.0, 1.0]}}, "sorted"),
        ({"variables": ["city"], "boundaries": {"city": [0.0, 1.0]}}, "numeric"),
        (
            {
                "variables": ["score"],
                "boundaries": {"score": [0.0, 1.0]},
                "output": "labels",
            },
            "labels",
        ),
        (
            {
                "variables": ["score"],
                "boundaries": {"score": [0.0, 1.0]},
                "out_of_range": "drop",
            },
            "out_of_range",
        ),
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
        ArbitraryDiscretiser(**kwargs).fit(dataset)


def test_transform_leaves_non_selected_columns_unchanged(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0, "north", 9.5), (2, 4.0, "south", 8.0)],
        schema="row_id INT, score DOUBLE, segment STRING, raw_value DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0]}
    ).fit(dataset)
    result = model.transform(dataset).orderBy("row_id").collect()

    assert [row.row_id for row in result] == [1, 2]
    assert [row.segment for row in result] == ["north", "south"]
    assert [row.raw_value for row in result] == [9.5, 8.0]
    assert [row.score for row in result] == [0, 1]


def test_arbitrary_transform_plan_remains_spark_native(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(1, 0.0), (2, 1.0), (3, 2.0), (4, 4.0)],
        schema="row_id INT, score DOUBLE",
    )

    model = ArbitraryDiscretiser(
        variables=["score"], boundaries={"score": [0.0, 2.0, 4.0]}
    ).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
