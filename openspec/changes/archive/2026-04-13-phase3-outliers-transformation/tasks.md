# Tasks: Phase 3 Outliers and Transformation

## Phase 1: Shared test-first guardrails
- [x] 1.1 Add contract tests in `tests/test_base_transformer.py` or focused validation modules for Phase 3 helper behavior covering selected-column resolution, numeric type checks, duplicate targets, learned trailing-underscore attrs, unsupported configuration, and Spark-native plan checks from `specs/outliers/spec.md` and `specs/transformation/spec.md`.
- [x] 1.2 Extend `src/spark_feature_engine/_validation.py` with reusable Phase 3 helpers for numeric column discovery, positivity validation, exponent normalization, learned-state checks, and outlier bound validation until the new Phase 1 tests pass.
- [x] 1.3 Update `src/spark_feature_engine/base.py` only as needed to support fitted estimator/model patterns and shared learned-state utilities required by Phase 3, keeping existing foundation behavior green.

## Phase 2: Outliers
- [x] 2.1 Write `tests/outliers/test_winsorizer.py` for learned capping thresholds, clipping behavior, future extreme values, non-selected-column stability, learned attrs, validation failures, and Spark-native execution from `specs/outliers/spec.md` (Winsorizer; Outlier Validation and Lifecycle; Native Spark Outlier Execution).
- [x] 2.2 Implement `src/spark_feature_engine/outliers/winsorizer.py` and `src/spark_feature_engine/outliers/__init__.py` so `Winsorizer.fit(df) -> WinsorizerModel` passes the new winsorizer tests with native Spark expressions only.
- [x] 2.3 Write `tests/outliers/test_outlier_trimmer.py` for learned trimming bounds, row-removal behavior, schema preservation, non-selected-column stability, learned attrs, validation failures, and Spark-native execution from `specs/outliers/spec.md` (OutlierTrimmer; Outlier Validation and Lifecycle; Native Spark Outlier Execution).
- [x] 2.4 Implement `src/spark_feature_engine/outliers/outlier_trimmer.py` so `OutlierTrimmer.fit(df) -> OutlierTrimmerModel` passes the new trimmer tests without Python UDFs.

## Phase 3: Transformation
- [x] 3.1 Write `tests/transformation/test_log_transformer.py` for positive-input log transforms, zero/negative rejection, non-selected-column stability, validation failures, and Spark-native execution from `specs/transformation/spec.md` (LogTransformer; Transformation Validation and Lifecycle; Native Spark Execution).
- [x] 3.2 Implement `src/spark_feature_engine/transformation/log_transformer.py` and `src/spark_feature_engine/transformation/__init__.py` so `LogTransformer` passes the new log-transform tests using native Spark expressions only.
- [x] 3.3 Write `tests/transformation/test_power_transformer.py` for default exponent behavior, explicit exponent behavior, non-selected-column stability, validation failures, and Spark-native execution from `specs/transformation/spec.md` (PowerTransformer; Transformation Validation and Lifecycle; Native Spark Execution).
- [x] 3.4 Implement `src/spark_feature_engine/transformation/power_transformer.py` so `PowerTransformer` passes the new power-transform tests without UDFs.

## Phase 4: Package integration and verification
- [x] 4.1 Update `src/spark_feature_engine/__init__.py` to export all Phase 3 outlier and transformation classes, and add any package-level import coverage needed in tests.
- [x] 4.2 Run targeted pytest modules after each feature area and then execute full verification with `pytest`, `mypy src tests`, and `python -m build`, confirming all Phase 1/2/3 suites remain green.
- [x] 4.3 Review representative Spark execution plans from the new outlier and transformation tests to confirm no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes appear in fit/transform plans.
