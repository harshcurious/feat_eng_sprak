# Tasks: Phase 4 Feature Creation

## Phase 1: Shared validation and lifecycle groundwork
- [ ] 1.1 Add Phase 4 helper tests in `tests/test_phase4_validation_helpers.py` and `tests/test_base_transformer.py` for supported math-function normalization, variable/reference resolution, output-name collision checks, max-value validation, and learned trailing-underscore state from `specs/creation/spec.md` (Creation Validation and Lifecycle; CyclicalFeatures).
- [ ] 1.2 Extend `src/spark_feature_engine/_validation.py` with reusable creation helpers for minimum-variable checks, supported function validation, reference resolution, generated-name validation, and max-value normalization until the new Phase 4 helper tests pass.
- [ ] 1.3 Update `src/spark_feature_engine/base.py` only if needed to support the fitted `CyclicalFeatures -> CyclicalFeaturesModel` lifecycle while keeping existing Phase 1-3 behavior green.

### Verification
- Run: `uv run pytest tests/test_phase4_validation_helpers.py tests/test_base_transformer.py`
- Expected: New helper and lifecycle tests pass without regressing the existing base contract.

## Phase 2: MathFeatures
- [ ] 2.1 Write `tests/creation/test_math_features.py` for aggregate feature generation, multiple-function output, explicit `new_variable_names`, `drop_original`, invalid configuration failures, non-selected-column stability, and Spark-native execution from `specs/creation/spec.md` (MathFeatures; Creation Validation and Lifecycle; Native Spark Creation Execution).
- [ ] 2.2 Implement `src/spark_feature_engine/creation/math_features.py` with Spark-native support for the approved aggregate functions and deterministic output naming so the new MathFeatures tests pass without Python UDFs.

### Verification
- Run: `uv run pytest tests/creation/test_math_features.py tests/test_phase4_validation_helpers.py`
- Expected: MathFeatures behavior is covered and green, including native-plan assertions.

## Phase 3: RelativeFeatures
- [ ] 3.1 Write `tests/creation/test_relative_features.py` for variable/reference combinations, supported arithmetic operations, `fill_value` behavior for zero denominators, `drop_original`, invalid configuration failures, non-selected-column stability, and Spark-native execution from `specs/creation/spec.md` (RelativeFeatures; Creation Validation and Lifecycle; Native Spark Creation Execution).
- [ ] 3.2 Implement `src/spark_feature_engine/creation/relative_features.py` using Spark-native arithmetic expressions and explicit zero-denominator handling so the new RelativeFeatures tests pass without Python UDFs.

### Verification
- Run: `uv run pytest tests/creation/test_relative_features.py tests/test_phase4_validation_helpers.py`
- Expected: RelativeFeatures tests pass, including division-by-zero contract and native-plan assertions.

## Phase 4: CyclicalFeatures
- [ ] 4.1 Write `tests/creation/test_cyclical_features.py` for learned versus configured `max_values`, sine/cosine output generation, learned `max_values_` exposure, `drop_original`, invalid configuration failures, non-selected-column stability, and Spark-native execution from `specs/creation/spec.md` (CyclicalFeatures; Creation Validation and Lifecycle; Native Spark Creation Execution).
- [ ] 4.2 Implement `src/spark_feature_engine/creation/cyclical_features.py` and `src/spark_feature_engine/creation/__init__.py` so `CyclicalFeatures.fit(df) -> CyclicalFeaturesModel` passes the new tests using native Spark aggregates and trigonometric expressions only.

### Verification
- Run: `uv run pytest tests/creation/test_cyclical_features.py tests/test_phase4_validation_helpers.py`
- Expected: CyclicalFeatures tests pass for both learned and configured maxima, with no Python execution nodes in representative plans.

## Phase 5: Package integration and verification
- [ ] 5.1 Update `src/spark_feature_engine/__init__.py` to export all approved Phase 4 creation classes and add any package-level import coverage required by the new tests.
- [ ] 5.2 Run targeted pytest modules after each feature area and then execute full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`, confirming Phases 1-4 remain green.
- [ ] 5.3 Review representative Spark execution plans from the new creation tests to confirm no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes appear in fit or transform plans.

### Verification
- Run: `uv run pytest && uv run mypy src tests && uv run python -m build`
- Expected: The full Phase 1-4 suite, typing checks, and build all pass after package integration.
