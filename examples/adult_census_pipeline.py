"""Train a baseline and an improved Adult Census model in Spark."""

import marimo

__generated_with = "0.23.4"
app = marimo.App(width="full", auto_download=["ipynb"])

with app.setup:
    import sys
    from pathlib import Path

    import marimo as mo
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType, StringType

    from spark_feature_engine import (
        CategoricalImputer,
        CountFrequencyEncoder,
        DropConstantFeatures,
        MeanMedianImputer,
        OneHotEncoder,
        RareLabelEncoder,
        SelectByShuffling,
        SmartCorrelatedSelection,
    )

    if __package__ in {None, ""}:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from download_adult_census import (
            KAGGLE_DATASET,
            RAW_TABLE_NAME,
            RAW_VIEW_NAME,
            build_spark_session,
            create_raw_assets,
            download_dataset,
            load_extracted_dataset,
        )
    else:
        from .download_adult_census import (
            KAGGLE_DATASET,
            RAW_TABLE_NAME,
            RAW_VIEW_NAME,
            build_spark_session,
            create_raw_assets,
            download_dataset,
            load_extracted_dataset,
        )

    LABEL_COLUMN = "income_label"
    EDUCATION_ORDER = {
        "Preschool": 0,
        "1st-4th": 1,
        "5th-6th": 2,
        "7th-8th": 3,
        "9th": 4,
        "10th": 5,
        "11th": 6,
        "12th": 7,
        "HS-grad": 8,
        "Some-college": 9,
        "Assoc-voc": 10,
        "Assoc-acdm": 11,
        "Bachelors": 12,
        "Masters": 13,
        "Prof-school": 14,
        "Doctorate": 15,
    }
    SEX_ORDER = {"Female": 0, "Male": 1}
    REQUESTED_ONE_HOT_VARIABLES = ("race", "marital_status", "relationship")
    IGNORED_FEATURES = ("education_num",)


@app.function
def with_income_label(
    dataset: DataFrame,
    *,
    source_column: str = "income",
    output_column: str = LABEL_COLUMN,
) -> DataFrame:
    """Create a binary numeric label column from the Adult Census target."""
    data_type = dataset.schema[source_column].dataType
    if isinstance(data_type, StringType):
        normalized = F.regexp_replace(F.trim(F.col(source_column)), r"\.", "")
        label = (
            F.when(normalized == F.lit(">50K"), F.lit(1))
            .when(normalized == F.lit("<=50K"), F.lit(0))
            .otherwise(F.lit(None).cast("int"))
        )
    else:
        label = F.col(source_column).cast("int")
    return dataset.withColumn(output_column, label.cast("int"))


@app.function
def numeric_and_categorical_features(
    dataset: DataFrame,
    *,
    label_column: str = LABEL_COLUMN,
) -> tuple[list[str], list[str]]:
    """Split feature columns by type while excluding the label column."""
    numeric_features: list[str] = []
    categorical_features: list[str] = []
    for field in dataset.schema.fields:
        if field.name == label_column:
            continue
        if isinstance(field.dataType, StringType):
            categorical_features.append(field.name)
        elif isinstance(field.dataType, NumericType):
            numeric_features.append(field.name)
    return numeric_features, categorical_features


@app.function
def assert_improvement(*, baseline_score: float, improved_score: float) -> None:
    """Fail loudly when the improved pipeline does not beat the baseline."""
    if improved_score <= baseline_score:
        raise RuntimeError(
            "The improved model did not beat the baseline "
            f"({improved_score:.4f} <= {baseline_score:.4f})."
        )


@app.function
def replace_unknown_categories(dataset: DataFrame) -> DataFrame:
    """Convert blank and question-mark string values to nulls."""
    projections = []
    for field in dataset.schema.fields:
        column = F.col(field.name)
        if isinstance(field.dataType, StringType):
            projections.append(
                F.when(F.trim(column).isin("", "?"), F.lit(None))
                .otherwise(F.trim(column))
                .alias(field.name)
            )
        else:
            projections.append(column)
    return dataset.select(*projections)


@app.function
def prepare_modeling_frame(dataset: DataFrame) -> DataFrame:
    """Normalize raw Adult Census data for train/test modeling."""
    cleaned = replace_unknown_categories(dataset)
    with_label = with_income_label(cleaned)
    return with_label.drop("income")


@app.function
def format_value(value: object) -> str:
    """Format a value for safe display inside markdown tables."""
    if value is None:
        return ""
    return str(value).replace("|", r"\|")


@app.function
def preview_markdown(dataset: DataFrame, title: str, *, limit: int = 5) -> str:
    """Render a small markdown preview for a Spark DataFrame."""
    rows = dataset.limit(limit).collect()
    columns = dataset.columns
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = [
        "| " + " | ".join(format_value(row[column]) for column in columns) + " |"
        for row in rows
    ]
    schema = ", ".join(
        f"{field.name}: {field.dataType.simpleString()}"
        for field in dataset.schema.fields
    )
    return "\n".join(
        [
            f"### {title}",
            f"Rows: {dataset.count()}",
            f"Schema: {schema}",
            "",
            header,
            separator,
            *body,
        ]
    )


@app.function
def evaluate_binary_model(model, dataset: DataFrame, *, label_column: str) -> float:
    """Evaluate a fitted binary classifier on a Spark DataFrame."""
    assembler = VectorAssembler(
        inputCols=[column for column in dataset.columns if column != label_column],
        outputCol="features",
    )
    prepared = assembler.transform(dataset).select(
        F.col(label_column).alias("label"),
        "features",
    )
    predictions = model.transform(prepared)
    evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    return float(evaluator.evaluate(predictions))


@app.function
def fit_logistic_regression(train: DataFrame, *, label_column: str):
    """Fit a logistic regression model on the provided training frame."""
    assembler = VectorAssembler(
        inputCols=[column for column in train.columns if column != label_column],
        outputCol="features",
    )
    prepared = assembler.transform(train).select(
        F.col(label_column).alias("label"),
        "features",
    )
    estimator = LogisticRegression(maxIter=80, regParam=0.0, elasticNetParam=0.0)
    return estimator.fit(prepared)


@app.function
def score_model_frame(train: DataFrame, test: DataFrame, *, label_column: str) -> float:
    """Fit logistic regression on a frame and return its ROC AUC."""
    model = fit_logistic_regression(train, label_column=label_column)
    return evaluate_binary_model(model, test, label_column=label_column)


@app.function
def build_baseline_frames(
    train: DataFrame,
    test: DataFrame,
) -> tuple[list[str], DataFrame, DataFrame]:
    """Prepare the numeric-only baseline feature set."""
    numeric_features, _ = numeric_and_categorical_features(train)
    numeric_train = train.select(*numeric_features, LABEL_COLUMN)
    numeric_test = test.select(*numeric_features, LABEL_COLUMN)
    imputer = MeanMedianImputer(
        variables=numeric_features,
        imputation_method="median",
    )
    imputer_model = imputer.fit(numeric_train)
    return (
        numeric_features,
        imputer_model.transform(numeric_train),
        imputer_model.transform(numeric_test),
    )


@app.function
def apply_categorical_imputer(
    train: DataFrame,
    test: DataFrame,
    *,
    categorical_features: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Fill missing categorical values in the train and test frames."""
    model = CategoricalImputer(variables=categorical_features, fill_value="missing")
    return model.transform(train), model.transform(test)


@app.function
def apply_numeric_imputer(
    train: DataFrame,
    test: DataFrame,
    *,
    numeric_features: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Fill missing numeric values in the train and test frames."""
    model = MeanMedianImputer(
        variables=numeric_features,
        imputation_method="median",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def drop_optional_columns(
    train: DataFrame,
    test: DataFrame,
    *,
    columns: tuple[str, ...] = IGNORED_FEATURES,
) -> tuple[DataFrame, DataFrame]:
    """Drop optional features from the train and test frames when present."""
    present_columns = [column for column in columns if column in train.columns]
    if not present_columns:
        return train, test
    return train.drop(*present_columns), test.drop(*present_columns)


@app.function
def map_categorical_column(
    dataset: DataFrame, *, column: str, mapping: dict[str, int]
) -> DataFrame:
    """Map a categorical string column to integer codes."""
    if column not in dataset.columns:
        return dataset

    items = []
    for category, code in mapping.items():
        items.extend((F.lit(category), F.lit(code)))
    mapped = F.create_map(*items)[F.col(column)]
    encoded = (
        F.when(F.col(column).isNull(), F.lit(None))
        .otherwise(mapped)
        .cast("int")
        .alias(column)
    )
    projections = [
        encoded if name == column else F.col(name) for name in dataset.columns
    ]
    return dataset.select(*projections)


@app.function
def apply_workclass_frequency_encoding(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Encode the workclass column using frequency encoding."""
    if "workclass" not in train.columns:
        return train, test
    model = CountFrequencyEncoder(
        variables=["workclass"],
        method="frequency",
        unseen="encode",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_education_ordinal_encoding(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Encode education with a semantically ordered ordinal mapping."""
    return (
        map_categorical_column(train, column="education", mapping=EDUCATION_ORDER),
        map_categorical_column(test, column="education", mapping=EDUCATION_ORDER),
    )


@app.function
def apply_sex_ordinal_encoding(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Encode sex with a deterministic ordinal mapping."""
    return (
        map_categorical_column(train, column="sex", mapping=SEX_ORDER),
        map_categorical_column(test, column="sex", mapping=SEX_ORDER),
    )


@app.function
def apply_native_country_binary_encoding(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Encode native_country as United-States versus anything else."""

    def _encode(dataset: DataFrame) -> DataFrame:
        if "native_country" not in dataset.columns:
            return dataset
        encoded = (
            F.when(F.col("native_country").isNull(), F.lit(None))
            .when(F.col("native_country") == F.lit("United-States"), F.lit(1))
            .otherwise(F.lit(0))
            .cast("int")
            .alias("native_country")
        )
        projections = [
            encoded if name == "native_country" else F.col(name)
            for name in dataset.columns
        ]
        return dataset.select(*projections)

    return _encode(train), _encode(test)


@app.function
def apply_requested_one_hot_encoding(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """One-hot encode race, marital_status, and relationship when present."""
    variables = [
        column for column in REQUESTED_ONE_HOT_VARIABLES if column in train.columns
    ]
    if not variables:
        return train, test
    model = OneHotEncoder(variables=variables)
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_requested_categorical_encodings(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Apply the requested Adult Census categorical encodings to both frames."""
    encoded_train, encoded_test = apply_workclass_frequency_encoding(train, test)
    encoded_train, encoded_test = apply_education_ordinal_encoding(
        encoded_train, encoded_test
    )
    encoded_train, encoded_test = apply_sex_ordinal_encoding(
        encoded_train, encoded_test
    )
    encoded_train, encoded_test = apply_native_country_binary_encoding(
        encoded_train, encoded_test
    )
    encoded_train, encoded_test = apply_requested_one_hot_encoding(
        encoded_train, encoded_test
    )
    return encoded_train, encoded_test


@app.function
def apply_rare_label_encoding(
    train: DataFrame,
    test: DataFrame,
    *,
    categorical_features: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Group infrequent categories in the train and test frames."""
    model = RareLabelEncoder(
        variables=categorical_features,
        tolerance=0.01,
        min_categories=5,
        replacement_label="Rare",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_frequency_encoding(
    train: DataFrame,
    test: DataFrame,
    *,
    categorical_features: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Replace categories with frequency encodings in both frames."""
    model = CountFrequencyEncoder(
        variables=categorical_features,
        method="frequency",
        unseen="encode",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_constant_selection(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Drop constant features from the train and test frames."""
    selector_variables = [column for column in train.columns if column != LABEL_COLUMN]
    model = DropConstantFeatures(
        variables=selector_variables,
        tol=1.0,
        missing_values="ignore",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_correlated_selection(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Drop highly correlated features from the train and test frames."""
    selector_variables = [column for column in train.columns if column != LABEL_COLUMN]
    model = SmartCorrelatedSelection(
        variables=selector_variables,
        threshold=0.98,
        selection_method="variance",
    )
    fitted = model.fit(train)
    return fitted.transform(train), fitted.transform(test)


@app.function
def apply_shuffle_selection(
    train: DataFrame,
    test: DataFrame,
) -> tuple[DataFrame, DataFrame, list[str]]:
    """Keep features that survive shuffle-based feature selection."""
    selector_variables = [column for column in train.columns if column != LABEL_COLUMN]
    model = SelectByShuffling(
        estimator=LogisticRegression(maxIter=40, regParam=0.0),
        target=LABEL_COLUMN,
        variables=selector_variables,
        scoring="roc_auc",
        threshold=0.0,
        cv=3,
        random_state=42,
    )
    fitted = model.fit(train)
    transformed_train = fitted.transform(train)
    transformed_test = fitted.transform(test)
    selected_features = [
        column for column in transformed_train.columns if column != LABEL_COLUMN
    ]
    return transformed_train, transformed_test, selected_features


@app.function
def train_baseline_model(train: DataFrame, test: DataFrame) -> float:
    """Train a numeric-only baseline benchmark."""
    _, baseline_train, baseline_test = build_baseline_frames(train, test)
    return score_model_frame(baseline_train, baseline_test, label_column=LABEL_COLUMN)


@app.function
def train_improved_model(train: DataFrame, test: DataFrame) -> tuple[float, list[str]]:
    """Train a richer pipeline with encoding and feature selection."""
    numeric_features, categorical_features = numeric_and_categorical_features(train)
    improved_train, improved_test = apply_categorical_imputer(
        train,
        test,
        categorical_features=categorical_features,
    )
    improved_train, improved_test = drop_optional_columns(improved_train, improved_test)
    numeric_features, _ = numeric_and_categorical_features(improved_train)
    improved_train, improved_test = apply_numeric_imputer(
        improved_train,
        improved_test,
        numeric_features=numeric_features,
    )
    improved_train, improved_test = apply_requested_categorical_encodings(
        improved_train,
        improved_test,
    )
    improved_train, improved_test = apply_constant_selection(
        improved_train,
        improved_test,
    )
    improved_train, improved_test = apply_correlated_selection(
        improved_train,
        improved_test,
    )
    improved_train, improved_test, selected_features = apply_shuffle_selection(
        improved_train,
        improved_test,
    )
    score = score_model_frame(improved_train, improved_test, label_column=LABEL_COLUMN)
    return score, selected_features


@app.function
def ensure_raw_table(
    spark: SparkSession,
    *,
    force_download: bool = False,
    dataset_slug: str = KAGGLE_DATASET,
) -> None:
    """Create the managed raw table when it does not exist yet."""
    if spark.catalog.tableExists(RAW_TABLE_NAME) and not force_download:
        spark.table(RAW_TABLE_NAME).createOrReplaceTempView(RAW_VIEW_NAME)
        return

    extracted_dir = download_dataset(dataset_slug=dataset_slug, force=force_download)
    dataset, _ = load_extracted_dataset(spark, extracted_dir=Path(extracted_dir))
    create_raw_assets(spark, dataset)


@app.function
def run_pipeline(*, force_download: bool = False) -> tuple[float, float, list[str]]:
    """Run the baseline vs improved modeling comparison."""
    spark = build_spark_session(app_name="adult-census-notebook")
    try:
        ensure_raw_table(spark, force_download=force_download)
        dataset = prepare_modeling_frame(spark.table(RAW_TABLE_NAME))
        train, test = dataset.randomSplit([0.8, 0.2], seed=42)
        baseline_score = train_baseline_model(train, test)
        improved_score, selected_features = train_improved_model(train, test)
        assert_improvement(
            baseline_score=baseline_score,
            improved_score=improved_score,
        )
        return baseline_score, improved_score, selected_features
    finally:
        spark.stop()


@app.cell(hide_code=True)
def _():
    """Show the notebook title and overview."""
    mo.md("""
    # Adult Census feature engineering notebook

    This notebook downloads the Kaggle Adult Census dataset, materializes Spark
    tables, and compares a baseline classifier with an improved feature-engineered
    pipeline.
    """)
    return


@app.cell
def _():
    """Create the notebook control for forcing a fresh download."""
    force_download = mo.ui.switch(value=False, label="Force Kaggle re-download")
    force_download
    return (force_download,)


@app.cell
def _(force_download):
    """Load the raw Adult Census table and preview it."""
    spark = build_spark_session(app_name="adult-census-notebook")
    ensure_raw_table(spark, force_download=force_download.value)
    raw_dataset = spark.table(RAW_TABLE_NAME)
    mo.vstack(
        [
            mo.md(preview_markdown(raw_dataset, "Raw dataset preview")),
            mo.md(f"Managed table: `{RAW_TABLE_NAME}`  \nTemp view: `{RAW_VIEW_NAME}`"),
        ]
    )
    return (raw_dataset,)


@app.cell
def _(raw_dataset):
    """Build and preview the cleaned modeling frame."""
    modeling_frame = prepare_modeling_frame(raw_dataset)
    mo.md(preview_markdown(modeling_frame, "Cleaned modeling frame"))
    return (modeling_frame,)


@app.cell
def _(modeling_frame):
    """Split the modeling frame into training and test sets."""
    train, test = modeling_frame.randomSplit([0.8, 0.2], seed=42)
    mo.md(
        f"Train rows: {train.count()}  \nTest rows: {test.count()}  \nLabel column: `{LABEL_COLUMN}`"
    )
    return test, train


@app.cell
def _(test, train):
    """Train and preview the numeric-only baseline model."""
    baseline_numeric_features, baseline_train, baseline_test = build_baseline_frames(
        train, test
    )
    baseline_score = score_model_frame(
        baseline_train,
        baseline_test,
        label_column=LABEL_COLUMN,
    )
    mo.vstack(
        [
            mo.md(
                f"### Baseline\nNumeric features: {', '.join(baseline_numeric_features)}"
            ),
            mo.md(preview_markdown(baseline_train, "Baseline training frame")),
            mo.md(f"Baseline ROC AUC: **{baseline_score:.4f}**"),
        ]
    )
    return (baseline_score,)


@app.cell
def _(test, train):
    """Apply categorical imputation and preview the first pipeline step."""
    _, step1_categorical_features = numeric_and_categorical_features(train)
    imputed_train_1, imputed_test_1 = apply_categorical_imputer(
        train,
        test,
        categorical_features=step1_categorical_features,
    )
    train_1, test_1 = drop_optional_columns(imputed_train_1, imputed_test_1)
    scored_train_1, scored_test_1 = apply_requested_categorical_encodings(
        train_1, test_1
    )
    step1_score = score_model_frame(
        scored_train_1, scored_test_1, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md(
                "### Step 1: Categorical imputation\n"
                f"Categorical features: {', '.join(step1_categorical_features)}\n"
                "Ignored feature: education_num"
            ),
            mo.md(preview_markdown(train_1, "After categorical imputation")),
            mo.md(f"Step 1 ROC AUC: **{step1_score:.4f}**"),
        ]
    )
    step1_numeric_features, _ = numeric_and_categorical_features(train_1)
    return step1_score, step1_numeric_features, test_1, train_1


@app.cell
def _(step1_numeric_features, test_1, train_1):
    """Apply numeric imputation and preview the second pipeline step."""
    train_2, test_2 = apply_numeric_imputer(
        train_1,
        test_1,
        numeric_features=step1_numeric_features,
    )
    scored_train_2, scored_test_2 = apply_requested_categorical_encodings(
        train_2, test_2
    )
    step2_score = score_model_frame(
        scored_train_2, scored_test_2, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md("### Step 2: Numeric imputation"),
            mo.md(preview_markdown(train_2, "After numeric imputation")),
            mo.md(f"Step 2 ROC AUC: **{step2_score:.4f}**"),
        ]
    )
    return step2_score, test_2, train_2


@app.cell
def _(test_2, train_2):
    """Apply workclass frequency encoding and preview the third pipeline step."""
    train_3, test_3 = apply_workclass_frequency_encoding(train_2, test_2)
    scored_train_3, scored_test_3 = apply_requested_categorical_encodings(
        train_3, test_3
    )
    step3_score = score_model_frame(
        scored_train_3, scored_test_3, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md("### Step 3: Workclass frequency encoding"),
            mo.md(preview_markdown(train_3, "After workclass frequency encoding")),
            mo.md(f"Step 3 ROC AUC: **{step3_score:.4f}**"),
        ]
    )
    return step3_score, test_3, train_3


@app.cell
def _(test_3, train_3):
    """Apply education ordinal encoding and preview the fourth pipeline step."""
    train_4, test_4 = apply_education_ordinal_encoding(train_3, test_3)
    scored_train_4, scored_test_4 = apply_requested_categorical_encodings(
        train_4, test_4
    )
    step4_score = score_model_frame(
        scored_train_4, scored_test_4, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md("### Step 4: Education ordinal encoding"),
            mo.md(preview_markdown(train_4, "After education ordinal encoding")),
            mo.md(f"Step 4 ROC AUC: **{step4_score:.4f}**"),
        ]
    )
    return step4_score, test_4, train_4


@app.cell
def _(test_4, train_4):
    """Apply sex ordinal encoding and preview the fifth pipeline step."""
    train_5, test_5 = apply_sex_ordinal_encoding(train_4, test_4)
    scored_train_5, scored_test_5 = apply_requested_categorical_encodings(
        train_5, test_5
    )
    step5_score = score_model_frame(
        scored_train_5, scored_test_5, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md("### Step 5: Sex ordinal encoding"),
            mo.md(preview_markdown(train_5, "After sex ordinal encoding")),
            mo.md(f"Step 5 ROC AUC: **{step5_score:.4f}**"),
        ]
    )
    return step5_score, test_5, train_5


@app.cell
def _(test_5, train_5):
    """Apply native country binary encoding and preview the sixth pipeline step."""
    train_6, test_6 = apply_native_country_binary_encoding(train_5, test_5)
    scored_train_6, scored_test_6 = apply_requested_categorical_encodings(
        train_6, test_6
    )
    step6_score = score_model_frame(
        scored_train_6, scored_test_6, label_column=LABEL_COLUMN
    )
    mo.vstack(
        [
            mo.md("### Step 6: Native country binary encoding"),
            mo.md(preview_markdown(train_6, "After native country binary encoding")),
            mo.md(f"Step 6 ROC AUC: **{step6_score:.4f}**"),
        ]
    )
    return step6_score, test_6, train_6


@app.cell
def _(test_6, train_6):
    """Apply one-hot encoding to selected categoricals and preview the seventh step."""
    train_7, test_7 = apply_requested_one_hot_encoding(train_6, test_6)
    step7_score = score_model_frame(train_7, test_7, label_column=LABEL_COLUMN)
    mo.vstack(
        [
            mo.md("### Step 7: One-hot encode race, marital_status, and relationship"),
            mo.md(preview_markdown(train_7, "After requested one-hot encoding")),
            mo.md(f"Step 7 ROC AUC: **{step7_score:.4f}**"),
        ]
    )
    return step7_score, test_7, train_7


@app.cell
def _(test_7, train_7):
    """Drop constant features and preview the eighth pipeline step."""
    train_8, test_8 = apply_constant_selection(train_7, test_7)
    step8_score = score_model_frame(train_8, test_8, label_column=LABEL_COLUMN)
    mo.vstack(
        [
            mo.md("### Step 8: Drop constant features"),
            mo.md(preview_markdown(train_8, "After constant-feature selection")),
            mo.md(f"Step 8 ROC AUC: **{step8_score:.4f}**"),
        ]
    )
    return step8_score, test_8, train_8


@app.cell
def _(test_8, train_8):
    """Drop correlated features and preview the ninth pipeline step."""
    train_9, test_9 = apply_correlated_selection(train_8, test_8)
    step9_score = score_model_frame(train_9, test_9, label_column=LABEL_COLUMN)
    mo.vstack(
        [
            mo.md("### Step 9: Correlation pruning"),
            mo.md(preview_markdown(train_9, "After correlated-feature selection")),
            mo.md(f"Step 9 ROC AUC: **{step9_score:.4f}**"),
        ]
    )
    return step9_score, test_9, train_9


@app.cell
def _(test_9, train_9):
    """Apply shuffle selection and preview the final pipeline step."""
    train_10, test_10, selected_features = apply_shuffle_selection(train_9, test_9)
    step10_score = score_model_frame(train_10, test_10, label_column=LABEL_COLUMN)
    mo.vstack(
        [
            mo.md("### Step 10: Shuffle-based selection"),
            mo.md(preview_markdown(train_10, "Final training frame")),
            mo.md(f"Selected features: {', '.join(selected_features)}"),
            mo.md(f"Step 10 ROC AUC: **{step10_score:.4f}**"),
        ]
    )
    return selected_features, step10_score


@app.cell(hide_code=True)
def _(
    baseline_score,
    selected_features,
    step1_score,
    step2_score,
    step3_score,
    step4_score,
    step5_score,
    step6_score,
    step7_score,
    step8_score,
    step9_score,
    step10_score,
):
    """Summarize the baseline and stepwise model scores."""
    assert_improvement(baseline_score=baseline_score, improved_score=step10_score)
    mo.md(
        f"""
        ## Comparison

        - Baseline ROC AUC: **{baseline_score:.4f}**
        - Step 1 ROC AUC: **{step1_score:.4f}**
        - Step 2 ROC AUC: **{step2_score:.4f}**
        - Step 3 ROC AUC: **{step3_score:.4f}**
        - Step 4 ROC AUC: **{step4_score:.4f}**
        - Step 5 ROC AUC: **{step5_score:.4f}**
        - Step 6 ROC AUC: **{step6_score:.4f}**
        - Step 7 ROC AUC: **{step7_score:.4f}**
        - Step 8 ROC AUC: **{step8_score:.4f}**
        - Step 9 ROC AUC: **{step9_score:.4f}**
        - Step 10 ROC AUC: **{step10_score:.4f}**
        - Selected features: **{len(selected_features)}**
        """
    )
    return


if __name__ == "__main__":
    app.run()
