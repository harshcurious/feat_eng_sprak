"""Contract tests for the Spark-native information value selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.information_value import (
    SelectByInformationValue,
    SelectByInformationValueModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_information_value_retains_only_features_above_threshold(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (0, 1.0, 1.0, "A", "same"),
            (0, 2.0, 1.0, "A", "same"),
            (0, 3.0, 1.0, "A", "same"),
            (1, 8.0, 1.0, "B", "same"),
            (1, 9.0, 1.0, "B", "same"),
            (1, 10.0, 1.0, "B", "same"),
        ],
        schema="target INT, shifted_num DOUBLE, stable_num DOUBLE, shifted_cat STRING, stable_cat STRING",
    )

    model = SelectByInformationValue(target="target", threshold=0.1, bins=3).fit(
        dataset
    )
    result = model.transform(dataset)

    assert isinstance(model, SelectByInformationValueModel)
    assert model.variables_ == [
        "shifted_num",
        "stable_num",
        "shifted_cat",
        "stable_cat",
    ]
    assert set(model.features_to_drop_) == {"stable_num", "stable_cat"}
    assert model.information_values_["shifted_num"] > 0.1
    assert model.information_values_["shifted_cat"] > 0.1
    assert model.information_values_["stable_num"] == pytest.approx(0.0)
    assert model.information_values_["stable_cat"] == pytest.approx(0.0)
    assert result.columns == ["target", "shifted_num", "shifted_cat"]


def test_information_value_supports_equal_frequency_binning(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (0, 1.0),
            (0, 2.0),
            (0, 3.0),
            (1, 8.0),
            (1, 9.0),
            (1, 10.0),
        ],
        schema="target INT, shifted_num DOUBLE",
    )

    model = SelectByInformationValue(
        target="target",
        variables=["shifted_num"],
        threshold=0.1,
        bins=3,
        strategy="equal_frequency",
    ).fit(dataset)

    assert model.features_to_drop_ == []
    assert model.information_values_["shifted_num"] > 0.1


def test_information_value_rejects_non_binary_target(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0), (2, 3.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(ValueError, match="binary"):
        SelectByInformationValue(target="target", variables=["shifted_num"]).fit(
            dataset
        )


@pytest.mark.parametrize(
    ("threshold", "bins", "strategy", "message"),
    [
        (-0.1, 3, "equal_width", "threshold"),
        (0.1, 1, "equal_width", "bins"),
        (0.1, 3, "mystery", "strategy"),
    ],
)
def test_information_value_rejects_invalid_configuration(
    spark_session,
    threshold,
    bins,
    strategy,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        SelectByInformationValue(
            target="target",
            variables=["shifted_num"],
            threshold=threshold,
            bins=bins,
            strategy=strategy,
        ).fit(dataset)


def test_information_value_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (0, 1.0, "A"),
            (0, 2.0, "A"),
            (1, 8.0, "B"),
            (1, 9.0, "B"),
        ],
        schema="target INT, shifted_num DOUBLE, shifted_cat STRING",
    )

    model = SelectByInformationValue(target="target", threshold=0.1, bins=2).fit(
        dataset
    )
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
