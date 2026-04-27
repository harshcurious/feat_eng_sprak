# Tasks: Phase 6 Advanced Feature Selection

## Phase 1: Shared supervised-selection groundwork
- [x] 1.1 Add Phase 6 helper tests for binary-target validation, supported scoring validation, estimator compatibility checks, deterministic fold assignment, configured drop validation, and learned trailing-underscore selector state.
- [x] 1.2 Extend shared validation and internal selection utilities to support binary-target checks, scoring validation, estimator compatibility, deterministic fold generation, and empty-output protection until the new helper tests pass.
- [x] 1.3 Update `src/spark_feature_engine/base.py` only if needed to support Phase 6 selector lifecycle and direct configured transformers while keeping Phases 1-5 green.

### Verification
- Run: `uv run pytest tests/test_phase6_validation_helpers.py tests/test_base_transformer.py`
- Expected: New helper and lifecycle tests pass without regressing the existing base contract.

## Phase 2: Configured and target-aware statistical selectors
- [x] 2.1 Write `tests/selection/test_drop_features.py` for configured dropping, invalid configuration failures, empty-output protection, and non-selected-column stability.
- [x] 2.2 Implement `src/spark_feature_engine/selection/drop_features.py` as a configured Spark-native transformer.
- [x] 2.3 Write `tests/selection/test_drop_psi_features.py` for binary-target PSI comparison, numerical binning, categorical PSI behavior, threshold-based drops, invalid target failures, and Spark-native execution.
- [x] 2.4 Implement `src/spark_feature_engine/selection/drop_psi_features.py` using Spark-native per-class distribution comparisons only.
- [x] 2.5 Write `tests/selection/test_information_value.py` for binary-target validation, categorical and numerical IV calculation, threshold-based drops, invalid configuration failures, and Spark-native execution.
- [x] 2.6 Implement `src/spark_feature_engine/selection/information_value.py` using Spark-native binning and WoE / IV aggregation only.
- [x] 2.7 Write `tests/selection/test_target_mean_selection.py` for per-feature target-mean performance, threshold behavior, binning behavior, invalid target failures, non-selected-column stability, and Spark-native execution.
- [x] 2.8 Implement `src/spark_feature_engine/selection/target_mean_selection.py` using leak-safe fold-wise target-mean encoding and Spark-native scoring.

### Verification
- Run: `uv run pytest tests/selection/test_drop_features.py tests/selection/test_drop_psi_features.py tests/selection/test_information_value.py tests/selection/test_target_mean_selection.py tests/test_phase6_validation_helpers.py`
- Expected: Configured and target-aware statistical selectors are covered and green.

## Phase 3: Single-feature and shuffle-based model selectors
- [x] 3.1 Write `tests/selection/test_single_feature_performance.py` for supported estimator validation, per-feature scoring, threshold behavior, classification-only validation, deterministic repeated fits, and Spark-native execution.
- [x] 3.2 Implement `src/spark_feature_engine/selection/single_feature_performance.py` using the shared Spark-native classifier compatibility layer.
- [x] 3.3 Write `tests/selection/test_shuffle_features.py` for baseline scoring, shuffle-performance drifts, threshold behavior, random-state determinism, invalid estimator failures, and Spark-native execution.
- [x] 3.4 Implement `src/spark_feature_engine/selection/shuffle_features.py` using deterministic validation-slice shuffling and Spark-native re-scoring.

### Verification
- Run: `uv run pytest tests/selection/test_single_feature_performance.py tests/selection/test_shuffle_features.py tests/test_phase6_validation_helpers.py`
- Expected: Single-feature and shuffle-based selector tests pass with deterministic learned metadata.

## Phase 4: Probe and recursive selectors
- [x] 4.1 Write `tests/selection/test_probe_feature_selection.py` for collective-mode probe generation, probe-threshold behavior, deterministic random-state handling, unsupported-distribution failures, estimator compatibility failures, and Spark-native execution.
- [x] 4.2 Implement `src/spark_feature_engine/selection/probe_feature_selection.py` in collective mode first using Spark-native probe generation and classifier feature-importance extraction.
- [x] 4.3 Write `tests/selection/test_recursive_feature_addition.py` for ranked-addition behavior, threshold handling, learned performance drifts, unsupported estimator failures, deterministic tie-breaking, and Spark-native execution.
- [x] 4.4 Implement `src/spark_feature_engine/selection/recursive_feature_addition.py` using classifier-derived feature ranking and deterministic incremental evaluation.
- [x] 4.5 Write `tests/selection/test_recursive_feature_elimination.py` for ranked-elimination behavior, threshold handling, learned removal drifts, unsupported estimator failures, deterministic tie-breaking, and Spark-native execution.
- [x] 4.6 Implement `src/spark_feature_engine/selection/recursive_feature_elimination.py` using classifier-derived feature ranking and deterministic recursive removal.

### Verification
- Run: `uv run pytest tests/selection/test_probe_feature_selection.py tests/selection/test_recursive_feature_addition.py tests/selection/test_recursive_feature_elimination.py tests/test_phase6_validation_helpers.py`
- Expected: Probe and recursive selector tests pass with stable learned state and native-plan assertions.

## Phase 5: Package integration and full verification
- [x] 5.1 Update `src/spark_feature_engine/selection/__init__.py`, `src/spark_feature_engine/__init__.py`, and `tests/test_package_setup.py` to export all approved Phase 6 selection classes.
- [x] 5.2 Run targeted pytest modules after each selector family and then execute full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`, confirming Phases 1-6 remain green.
- [x] 5.3 Review representative Spark execution plans from the new Phase 6 tests to confirm no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes appear in fit, score, or transform plans.
- [x] 5.4 Keep MRMR deferred as a TODO only; do not implement `MRMR` in this change.

### Verification
- Run: `uv run pytest && uv run mypy src tests && uv run python -m build`
- Expected: The full Phase 1-6 suite, typing checks, and build all pass after advanced selection integration.
