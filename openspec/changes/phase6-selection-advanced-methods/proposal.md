# Proposal: Phase 6 Advanced Feature Selection

## Intent
Extend `spark-feature-engine` with the remaining approved feature-selection methods from `feature_engine.selection`, adapted for Spark-native execution and the existing `pyspark.ml`-style API. This phase focuses on classification-only behavior, keeps MRMR out of scope, and preserves the project rules around test-first development, no Python UDFs, and DataFrame-native execution.

## Scope
### In Scope
- Feature selection support for:
  - `DropFeatures`
  - `DropHighPSIFeatures`
  - `SelectByInformationValue`
  - `SelectByTargetMeanPerformance`
  - `SelectBySingleFeaturePerformance`
  - `SelectByShuffling`
  - `ProbeFeatureSelection` in collective mode first
  - `RecursiveFeatureAddition`
  - `RecursiveFeatureElimination`
- Classification-only behavior for all target-aware or model-based selectors.
- Support for Spark ML classifiers and other classifiers that operate natively on Spark DataFrames, such as Spark-compatible XGBoost integrations.
- Shared evaluation and validation helpers needed to score classifiers, compare candidate feature sets, and expose learned trailing-underscore state.
- Public package exports for the new Phase 6 APIs.
- Test-first development for helper abstractions and each selector.
- Spark-native fit and transform flows with no Python UDFs.

### Out of Scope
- `MRMR`.
- Regression support.
- Pandas-only, NumPy-only, or scikit-learn-only estimator integrations.
- Wrapper support for estimators that require local array conversion.
- Non-native execution paths that rely on Python UDFs, `toPandas()`, or Arrow-based feature evaluation.

## Approach
Implement Phase 6 as an expansion of the `selection` domain. Most selectors in this phase learn a drop set or selected feature set from data and should follow the established estimator-model lifecycle. `DropFeatures` is the one exception: it is a deterministic configured dropper and should remain a direct transformer with validated configuration rather than inventing a meaningless fit step.

For target-aware and model-based selectors, add a small internal compatibility layer that can train, score, and extract feature importance from Spark-DataFrame-native classifiers. This layer should support Spark ML first and allow additional Spark-native classifier implementations when they expose equivalent fit / transform / importance behavior. `ProbeFeatureSelection` will ship in collective mode first and rely on estimator-level feature importance rather than the single-feature scoring variant.

`DropHighPSIFeatures` should compare each feature's distribution across target classes rather than across two arbitrary datasets or time splits. `SelectByInformationValue` and `SelectByTargetMeanPerformance` should support binary classification only in this phase. The model-based selectors should evaluate candidate features against classification metrics using Spark-native folds, deterministic ordering, and explicit tie-breaking.

## Affected Areas
- `src/spark_feature_engine/__init__.py` and package exports for selection APIs.
- New modules under `src/spark_feature_engine/selection/`.
- Shared helper additions in `src/spark_feature_engine/_validation.py` and likely new internal utilities for metric evaluation, fold handling, feature importance extraction, and target-aware binning.
- Possible small extensions to `src/spark_feature_engine/base.py` for selector lifecycle or configurable transformer support.
- New pytest modules under `tests/selection/`.
- Existing package-level import and typing surfaces impacted by new public classes.

## Risks
- Model-based selectors can become expensive because they retrain multiple classifiers per fit.
- Estimator compatibility is broader than Phase 5 and needs a clear contract for scoring, probabilities, feature importances, and deterministic behavior.
- PSI, information value, and target-mean selectors need explicit binary-target and missing-value contracts to avoid silent mismatches with the reference library.
- Recursive selectors need deterministic ranking and stopping behavior to keep tests stable.
- Probe-based selection introduces randomness and must make reproducibility explicit.

## Rollback Plan
If Phase 6 proves unstable, revert the new selection modules, tests, exports, and shared helper additions together while preserving the already-complete Phases 1-5 behavior. If the model-based selector family is the main source of instability, remove that family while keeping the simpler non-model selectors isolated.

## Success Criteria
- The library exposes Spark-native equivalents for the approved Phase 6 selectors, excluding MRMR.
- Classification-only constraints are explicit and enforced by validation.
- Target-aware and model-based selectors operate on Spark DataFrames without Python UDFs or local materialization.
- Tests written before implementation cover selector behavior, helper abstractions, target validation, deterministic learned state, estimator compatibility contracts, and representative native-plan checks.
- `pytest`, `mypy src tests`, and `python -m build` remain viable verification commands after Phase 6 is implemented.
