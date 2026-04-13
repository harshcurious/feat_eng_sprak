# Tasks: Phase 5 Core Filter Selection

## Phase 1: Shared selection validation and lifecycle groundwork
- [x] 1.1 Add Phase 5 helper tests in `tests/test_phase5_validation_helpers.py` and `tests/test_base_transformer.py` for threshold validation, supported selection-method validation, minimum-variable checks, deterministic drop-list behavior, and learned trailing-underscore selector state from `specs/selection/spec.md` (Selector Validation and Lifecycle; SmartCorrelatedSelection).
- [x] 1.2 Extend `src/spark_feature_engine/_validation.py` with reusable selection helpers for threshold normalization, selection-method validation, selector drop-set validation, and numeric multi-column resolution until the new Phase 5 helper tests pass.
- [x] 1.3 Update `src/spark_feature_engine/base.py` only if needed to support a consistent learned selector estimator-model lifecycle while keeping Phases 1-4 green.

### Verification
- Run: `uv run pytest tests/test_phase5_validation_helpers.py tests/test_base_transformer.py`
- Expected: New Phase 5 helper and selector-lifecycle tests pass without regressing the existing base contract.

## Phase 2: DropConstantFeatures
- [x] 2.1 Write `tests/selection/test_drop_constant_features.py` for constant and quasi-constant dropping, missing-value policy behavior, deterministic learned `features_to_drop_`, empty-output protection, non-selected-column stability, and Spark-native execution from `specs/selection/spec.md` (DropConstantFeatures; Selector Validation and Lifecycle; Native Spark Selection Execution).
- [x] 2.2 Implement `src/spark_feature_engine/selection/drop_constant_features.py` and `src/spark_feature_engine/selection/__init__.py` so `DropConstantFeatures.fit(df)` learns a deterministic drop set using native Spark operations only.

### Verification
- Run: `uv run pytest tests/selection/test_drop_constant_features.py tests/test_phase5_validation_helpers.py`
- Expected: DropConstantFeatures behavior is covered and green, including native-plan assertions.

## Phase 3: DropDuplicateFeatures
- [x] 3.1 Write `tests/selection/test_drop_duplicate_features.py` for duplicate-column detection, deterministic representative retention, learned duplicate groups, invalid configuration failures, non-selected-column stability, and Spark-native execution from `specs/selection/spec.md` (DropDuplicateFeatures; Selector Validation and Lifecycle; Native Spark Selection Execution).
- [x] 3.2 Implement `src/spark_feature_engine/selection/drop_duplicate_features.py` using Spark-native comparison and deterministic grouping logic so the new tests pass without Python UDFs.

### Verification
- Run: `uv run pytest tests/selection/test_drop_duplicate_features.py tests/test_phase5_validation_helpers.py`
- Expected: DropDuplicateFeatures tests pass with deterministic learned drop sets and native-plan assertions.

## Phase 4: Correlation selectors
- [x] 4.1 Write `tests/selection/test_drop_correlated_features.py` for numeric correlation filtering, learned correlated sets, threshold validation, deterministic drops, invalid non-numeric targets, non-selected-column stability, and Spark-native execution from `specs/selection/spec.md` (DropCorrelatedFeatures; Selector Validation and Lifecycle; Native Spark Selection Execution).
- [x] 4.2 Implement `src/spark_feature_engine/selection/drop_correlated_features.py` using Spark-native correlation analysis and deterministic pairwise drop rules so the new tests pass without Python UDFs.
- [x] 4.3 Write `tests/selection/test_smart_correlated_selection.py` for grouped correlation selection with `missing_values`, `cardinality`, and `variance`, learned representative features, unsupported-method rejection, deterministic tie-breaking, non-selected-column stability, and Spark-native execution from `specs/selection/spec.md` (SmartCorrelatedSelection; Selector Validation and Lifecycle; Native Spark Selection Execution).
- [x] 4.4 Implement `src/spark_feature_engine/selection/smart_correlated_selection.py` so `SmartCorrelatedSelection.fit(df)` keeps one representative per correlated group using the approved non-target heuristics only.

### Verification
- Run: `uv run pytest tests/selection/test_drop_correlated_features.py tests/selection/test_smart_correlated_selection.py tests/test_phase5_validation_helpers.py`
- Expected: Correlation-based selector tests pass, including deterministic grouping and native-plan assertions.

## Phase 5: Package integration and full verification
- [x] 5.1 Update `src/spark_feature_engine/__init__.py` and `tests/test_package_setup.py` to export all approved Phase 5 selection classes and cover package-level imports.
- [x] 5.2 Run targeted pytest modules after each selector area and then execute full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`, confirming Phases 1-5 remain green.
- [x] 5.3 Review representative Spark execution plans from the new selection tests to confirm no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes appear in fit or transform plans.
- [x] 5.4 Keep Phase 6 creation-family planning deferred by carrying a TODO note only; do not implement `GeoDistanceFeatures` or `DecisionTreeFeatures` in this change.

### Verification
- Run: `uv run pytest && uv run mypy src tests && uv run python -m build`
- Expected: The full Phase 1-5 suite, typing checks, and build all pass after selection integration.
