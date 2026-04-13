# Design: Phase 3 Outliers and Transformation

## Goals
- Add Spark-native implementations for `Winsorizer`, `OutlierTrimmer`, `LogTransformer`, and `PowerTransformer`.
- Keep PySpark ML semantics: `Estimator`/`Transformer` where fit-time state is required, `Transformer` otherwise.
- Use only Spark SQL/DataFrame expressions; no Python UDFs.
- Reuse Phase 1/2 base and validation helpers.

## Architecture Decisions

### Package layout
- Add `src/spark_feature_engine/outliers/` for outlier handling.
- Add `src/spark_feature_engine/transformation/` for numerical transforms.
- Export new public classes from `src/spark_feature_engine/__init__.py` and each subpackage `__init__.py`.

### Outliers
- `Winsorizer` will be an `Estimator` that learns capping thresholds from the training data and returns a fitted model.
- `OutlierTrimmer` will be an `Estimator` if the trimming bounds are learned from data; if implemented with fixed bounds, keep it a `Transformer`. The intended design is estimator/model to align with reference semantics and fit/transform lifecycle tests.
- Learned state should be stored with trailing-underscore attributes only (for example, per-column bounds maps).
- Transform logic will use native Spark functions like `when`, `least`, `greatest`, `col`, and boolean row filters for trimming.

### Transformations
- `LogTransformer` will be a stateless `Transformer`.
- `PowerTransformer` will be a stateless `Transformer` unless a specific reference behavior requires learned parameters; current scope assumes direct exponent-based transform.
- Both transforms will operate only on resolved numeric columns and preserve non-selected columns unchanged.
- Log validation will reject zero/negative inputs consistently before expression construction.

### Shared helpers
- Reuse `BaseSparkTransformer`, `BaseSparkEstimator`, and `BaseSparkModel`.
- Reuse `_validation.py` column resolution and type validation helpers.
- Add new validation helpers only if required for outlier bounds, positivity checks, or exponent normalization.

## Test Strategy

Tests will be written before implementation, grouped by feature area:

### Outliers
- Fit/transform happy paths for winsorization and trimming.
- Explicit and default variable resolution.
- Numeric-column validation and missing-column failures.
- Learned attribute checks for fitted models.
- Native Spark execution verification by asserting no `PythonUDF` / `BatchEvalPython` in the plan.
- Row-count changes for trimming and clipping behavior for winsorization.

### Transformation
- Log transform on positive numeric inputs.
- Rejection of zero/negative values.
- Power transform with default and explicit exponent behavior.
- Preservation of untouched columns and schema stability.
- Native Spark execution verification.

### General
- Use the existing Spark pytest fixture.
- Mirror current repository test style: compact contract tests, explicit assertions, and parametrized validation cases.

## Planned File Changes
- `src/spark_feature_engine/outliers/__init__.py`
- `src/spark_feature_engine/outliers/winsorizer.py`
- `src/spark_feature_engine/outliers/outlier_trimmer.py`
- `src/spark_feature_engine/transformation/__init__.py`
- `src/spark_feature_engine/transformation/log_transformer.py`
- `src/spark_feature_engine/transformation/power_transformer.py`
- `src/spark_feature_engine/__init__.py`
- `tests/outliers/test_winsorizer.py`
- `tests/outliers/test_outlier_trimmer.py`
- `tests/transformation/test_log_transformer.py`
- `tests/transformation/test_power_transformer.py`

## Notes
- Keep implementation Spark-native and deterministic.
- Prefer simple, explicit model state to match the trailing-underscore convention already established in the repo.
