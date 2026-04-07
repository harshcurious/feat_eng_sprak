# Tasks: Phase 1 Foundation and Imputation

## Phase 1: Project setup and quality gates
- [x] 1.1 Create `pyproject.toml` with package metadata, `src/` layout, pytest configuration, Ruff format/lint settings (line length 88), mypy settings, and documented local quality commands to satisfy `foundation/spec.md` Package Quality Gates.
- [x] 1.2 Create the Phase 1 package skeleton in `src/spark_feature_engine/__init__.py` and `src/spark_feature_engine/imputation/__init__.py` with explicit public exports for the in-scope transformers described in `design.md`.
- [x] 1.3 Create `tests/conftest.py` with a reusable local `SparkSession` fixture configured for deterministic pytest execution to satisfy `foundation/spec.md` Spark Test Harness.
- [x] 1.4 Add a lightweight import/configuration test module (for example `tests/test_package_setup.py`) that verifies the package import surface and shared Spark fixture wiring before implementation code is added.

## Phase 2: Shared transformer foundation
- [x] 2.1 Write `tests/test_base_transformer.py` first to cover the `BaseSparkTransformer` contract: Param-based configuration, copy behavior, variable resolution, validation failures, and trailing-underscore learned-state expectations from `foundation/spec.md` Base Transformer Contract and Learned State Naming.
- [x] 2.2 Implement `src/spark_feature_engine/base.py` with `BaseSparkTransformer` built on `pyspark.ml.Transformer` + `Params` (+ Spark ML read/write mixins as designed), including typed param helpers and selected-column resolution.
- [x] 2.3 Implement `src/spark_feature_engine/_validation.py` with shared schema/type validation utilities for numeric, categorical, missing-column, and general variable checks used by both transformers and estimators.
- [x] 2.4 Update package exports and public signatures so the shared foundation has full type hints and is ready for imputer reuse, satisfying `foundation/spec.md` Public API Typing.

## Phase 3: Learned numerical imputation
- [x] 3.1 Write `tests/imputation/test_mean_median.py` first to cover mean strategy, median strategy, auto-discovered vs explicit `variables`, non-selected columns unchanged, invalid target columns rejected, and fitted trailing-underscore attributes from `imputation/spec.md` scenarios for mean/median, column-scoped behavior, and parameter/schema validation.
- [x] 3.2 Implement `src/spark_feature_engine/imputation/mean_median.py` with a `MeanMedianImputer` estimator and fitted model class that learn per-column statistics using native Spark aggregations only (`mean`, `percentile_approx`) and expose `variables_` / `imputer_dict_`.
- [x] 3.3 Add or refine tests asserting the fitted model uses native Spark SQL/DataFrame operations only and does not rely on Python UDFs, satisfying `imputation/spec.md` Native Spark Execution.

## Phase 4: Stateless imputation transformers
- [x] 4.1 Write `tests/imputation/test_arbitrary_number.py` first to cover selected numeric-column filling, unchanged non-selected columns, invalid numeric-column validation, and Param handling for `imputation/spec.md` Arbitrary Numerical Imputation, Column-Scoped Behavior, and Parameter and Schema Validation.
- [x] 4.2 Implement `src/spark_feature_engine/imputation/arbitrary_number.py` with a typed transformer that fills selected numeric columns through native `DataFrame.na.fill` behavior only.
- [x] 4.3 Write `tests/imputation/test_categorical.py` first to cover default fill value `"missing"`, custom fill values, selected-column scope, and categorical validation for `imputation/spec.md` Categorical Imputation, Column-Scoped Behavior, and Parameter and Schema Validation.
- [x] 4.4 Implement `src/spark_feature_engine/imputation/categorical.py` with a typed transformer that fills selected categorical columns using native Spark operations only.
- [x] 4.5 Write `tests/imputation/test_drop_missing_data.py` first to cover row removal for selected columns only, unchanged behavior for non-selected columns, and invalid-column validation for `imputation/spec.md` Drop Missing Data, Column-Scoped Behavior, and Parameter and Schema Validation.
- [x] 4.6 Implement `src/spark_feature_engine/imputation/drop_missing_data.py` with a typed transformer that removes rows containing nulls in selected columns via native `DataFrame.na.drop` or equivalent native filtering.

## Phase 5: Integration and verification
- [x] 5.1 Verify all public Phase 1 exports from `src/spark_feature_engine/__init__.py` and `src/spark_feature_engine/imputation/__init__.py` match the implemented classes and fitted-model visibility decision from `design.md`.
- [x] 5.2 Run `ruff format --check .` and fix any formatting issues.
- [x] 5.3 Run `ruff check .` and fix any lint violations.
- [-] 5.4 Run `mypy src tests` and fix any public API or fixture typing issues. (Skipped: `mypy` is unavailable in the current environment and no viable project-local invocation exists.)
- [-] 5.5 Run `pytest` and confirm the Spark fixture, base transformer tests, and all imputation test modules pass together. (Skipped: `pytest` is unavailable in the current environment and no viable project-local invocation exists.)
- [x] 5.6 Perform final code review against `foundation/spec.md` and `imputation/spec.md` to confirm zero-UDF execution, test-first completion per feature area, and trailing-underscore learned attributes before handoff.
