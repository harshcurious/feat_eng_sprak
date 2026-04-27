"""Contract tests for the Spark-native PSI feature selector."""

from __future__ import annotations

import pytest

from spark_feature_engine.selection.drop_psi_features import (
    DropHighPSIFeatures,
    DropHighPSIFeaturesModel,
)


def _plan_text(dataset) -> str:
    return "\n".join(
        [
            dataset._jdf.queryExecution().optimizedPlan().toString(),
            dataset._jdf.queryExecution().executedPlan().toString(),
        ]
    )


def test_drop_high_psi_features_learns_and_removes_shifted_columns(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [
            (0, 10.0, 1.0, "A", "same"),
            (0, 11.0, 1.0, "A", "same"),
            (0, 12.0, 1.0, "A", "same"),
            (1, 100.0, 1.0, "B", "same"),
            (1, 101.0, 1.0, "B", "same"),
            (1, 102.0, 1.0, "B", "same"),
        ],
        schema="target INT, shifted_num DOUBLE, stable_num DOUBLE, shifted_cat STRING, stable_cat STRING",
    )

    model = DropHighPSIFeatures(target="target", threshold=0.2, bins=3).fit(dataset)
    result = model.transform(dataset)

    assert isinstance(model, DropHighPSIFeaturesModel)
    assert model.variables_ == [
        "shifted_num",
        "stable_num",
        "shifted_cat",
        "stable_cat",
    ]
    assert set(model.features_to_drop_) == {"shifted_num", "shifted_cat"}
    assert model.psi_values_["shifted_num"] > 0.2
    assert model.psi_values_["shifted_cat"] > 0.2
    assert model.psi_values_["stable_num"] == pytest.approx(0.0)
    assert model.psi_values_["stable_cat"] == pytest.approx(0.0)
    assert result.columns == ["target", "stable_num", "stable_cat"]


def test_drop_high_psi_features_supports_equal_frequency_binning(spark_session) -> None:
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

    model = DropHighPSIFeatures(
        target="target",
        variables=["shifted_num"],
        threshold=0.2,
        bins=3,
        strategy="equal_frequency",
    ).fit(dataset)

    assert model.features_to_drop_ == ["shifted_num"]


def test_drop_high_psi_features_rejects_missing_values_when_requested(
    spark_session,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, None), (1, 1.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(ValueError, match="missing"):
        DropHighPSIFeatures(
            target="target",
            variables=["shifted_num"],
            missing_values="raise",
        ).fit(dataset)


def test_drop_high_psi_features_rejects_non_binary_target(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0), (2, 3.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises(ValueError, match="binary"):
        DropHighPSIFeatures(target="target", variables=["shifted_num"]).fit(dataset)


@pytest.mark.parametrize(
    ("threshold", "bins", "strategy", "missing_values", "message"),
    [
        (-0.1, 3, "equal_width", "ignore", "threshold"),
        (0.2, 1, "equal_width", "ignore", "bins"),
        (0.2, 3, "mystery", "ignore", "strategy"),
        (0.2, 3, "equal_width", "include", "missing_values"),
    ],
)
def test_drop_high_psi_features_rejects_invalid_configuration(
    spark_session,
    threshold,
    bins,
    strategy,
    missing_values,
    message: str,
) -> None:
    dataset = spark_session.createDataFrame(
        [(0, 1.0), (1, 2.0)], schema="target INT, shifted_num DOUBLE"
    )

    with pytest.raises((TypeError, ValueError), match=message):
        DropHighPSIFeatures(
            target="target",
            variables=["shifted_num"],
            threshold=threshold,
            bins=bins,
            strategy=strategy,
            missing_values=missing_values,
        ).fit(dataset)


def test_drop_high_psi_features_use_native_spark_execution(spark_session) -> None:
    dataset = spark_session.createDataFrame(
        [
            (0, 10.0, "A"),
            (0, 11.0, "A"),
            (1, 100.0, "B"),
            (1, 101.0, "B"),
        ],
        schema="target INT, shifted_num DOUBLE, shifted_cat STRING",
    )

    model = DropHighPSIFeatures(target="target", threshold=0.2, bins=2).fit(dataset)
    transformed = model.transform(dataset)
    plan_text = _plan_text(transformed)

    assert "PythonUDF" not in plan_text
    assert "BatchEvalPython" not in plan_text
    assert "ArrowEvalPython" not in plan_text
