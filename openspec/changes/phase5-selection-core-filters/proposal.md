# Proposal: Phase 5 Core Filter Selection

## Intent
Extend `spark-feature-engine` with a first Spark-native feature-selection phase focused on core filter selectors. This phase should add PySpark-first equivalents of the selected `feature_engine.selection` filters while preserving Spark ML lifecycle semantics, Param-based configuration, test-first development, and zero-UDF execution.

## Scope
### In Scope
- Feature selection support for:
  - `DropConstantFeatures`
  - `DropDuplicateFeatures`
  - `DropCorrelatedFeatures`
  - `SmartCorrelatedSelection` limited to non-target modes:
    - `missing_values`
    - `cardinality`
    - `variance`
- Test-first development for each selector and shared selection validation behavior.
- Public package exports for the new selection APIs.
- Reuse or extension of the existing base estimator/model/transformer contract and shared validation helpers where needed.
- Native Spark SQL and DataFrame operations only; no Python UDFs.

### Out of Scope
- Any target-aware, label-aware, or model-based selectors.
- `SmartCorrelatedSelection` modes based on target correlation or model performance.
- PSI, single-feature performance, shuffle-based, recursive, or MRMR selectors.
- Phase 6 creation-family decisions. These remain deferred as a TODO for later planning.

## Approach
Implement Phase 5 as a dedicated `selection` domain with learned selectors that fit on a DataFrame and transform it by dropping identified features. `DropConstantFeatures` should learn columns whose predominant value frequency meets a configured threshold. `DropDuplicateFeatures` should learn exact duplicate columns using Spark-native comparisons and deterministic grouping. `DropCorrelatedFeatures` should learn correlated numeric columns from a pairwise correlation analysis and drop one side of each pair using a clear deterministic rule. `SmartCorrelatedSelection` should build correlated groups and keep one representative per group using non-target heuristics only. Tests should be written before implementation to lock down learned-state behavior, selector outputs, drop contracts, configuration validation, deterministic tie-breaking, and non-selected-column stability.

## Affected Areas
- `src/spark_feature_engine/__init__.py` and package exports for selection APIs.
- New modules under `src/spark_feature_engine/selection/`.
- Possible extensions to `src/spark_feature_engine/base.py` and `src/spark_feature_engine/_validation.py` for selector lifecycle, learned drop lists, and correlation-related validation helpers.
- New pytest modules under `tests/selection/`.
- Existing package-level import and typing surfaces impacted by new public classes.

## Risks
- Pairwise correlation logic can be expensive and must stay deterministic across tied relationships and grouped drops.
- Duplicate-feature detection across many columns can become costly if implemented with repeated scans instead of batched comparisons.
- Constant-feature detection needs a clear missing-value policy to avoid silent behavior mismatches with the reference library.
- `SmartCorrelatedSelection` has richer grouping and tie-breaking behavior than the simpler filters, so its contract needs to be explicit.
- Feature-dropping selectors can easily create empty output schemas if validation is weak.

## Rollback Plan
If Phase 5 proves unstable, revert the new selection modules, tests, exports, and any shared-helper additions together while preserving the already-complete Phases 1-4 functionality. If one selector family is problematic, remove that selector while keeping the rest of the phase isolated behind package exports.

## Success Criteria
- The library exposes Spark-native `DropConstantFeatures`, `DropDuplicateFeatures`, `DropCorrelatedFeatures`, and `SmartCorrelatedSelection` classes through the package root and selection subpackage.
- All new behavior executes through native PySpark operations only, with no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes in representative plans.
- Tests written before implementation cover happy paths, lifecycle behavior, learned trailing-underscore state, variable selection, deterministic feature dropping, validation failures, empty-output protections, and non-selected-column stability.
- `pytest`, `mypy src tests`, and `python -m build` remain viable local verification commands after Phase 5 is implemented.

## Deferred TODO
- Decide Phase 6 scope for the remaining creation families later.
