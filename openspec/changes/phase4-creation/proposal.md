# Proposal: Phase 4 Feature Creation

## Intent
Extend `spark-feature-engine` with the approved Phase 4 feature-creation primitives for Spark-native derived feature generation. This phase should add PySpark-first equivalents of the selected `feature_engine.creation` transformers while preserving Spark ML lifecycle semantics, Param-based configuration, test-first development, and zero-UDF execution.

## Scope
### In Scope
- Feature creation support for:
  - `MathFeatures`
  - `RelativeFeatures`
  - `CyclicalFeatures`
- Test-first development for each feature area using the existing local Spark pytest fixture and repository verification commands.
- Public package exports for the new creation classes.
- Reuse and extension of the existing base transformer contract and shared validation helpers where needed.
- Native Spark SQL and DataFrame expression implementations only; no Python UDFs.

### Out of Scope
- `GeoDistanceFeatures` and `DecisionTreeFeatures`.
- Additional feature-creation families beyond the approved Phase 4 slice.
- Broad package redesign, serialization hardening, or performance work beyond choosing native Spark expressions.
- CI or release automation changes outside what is needed to support this phase's tests and exports.

## Approach
Implement Phase 4 as a focused creation domain with three complementary transformers. `MathFeatures` should create aggregate features across multiple numeric columns using native Spark row-wise expressions. `RelativeFeatures` should create pairwise features between selected variables and reference variables using supported arithmetic operations and explicit output naming rules. `CyclicalFeatures` should learn or accept maximum values per variable and generate sine and cosine projections using Spark-native trigonometric expressions. Tests should be written before implementation to lock down lifecycle behavior, variable selection, naming, drop-original handling, validation failures, learned-state behavior where applicable, and non-selected-column stability.

## Affected Areas
- `src/spark_feature_engine/__init__.py` and package exports for creation APIs.
- New modules under `src/spark_feature_engine/creation/`.
- Possible extensions to `src/spark_feature_engine/base.py` and `src/spark_feature_engine/_validation.py` for multi-column creation validation, naming helpers, numeric checks, and learned-state helpers.
- New pytest modules under `tests/creation/`.
- Existing package-level import and typing surfaces impacted by new public classes.

## Risks
- Row-wise aggregate behavior in Spark must stay deterministic across null-handling and selected-column combinations.
- `RelativeFeatures` can expand columns quickly; naming and duplicate-output protection need a clear contract.
- Division, modulo, and exponent operations need explicit handling for zero denominators and invalid numeric cases.
- `CyclicalFeatures` mixes learned state and configurable overrides, which can create ambiguous fit/transform semantics if not specified clearly.
- Matching practical `feature_engine` behavior while staying idiomatic to Spark ML may require deliberate API adaptations.

## Rollback Plan
If Phase 4 proves unstable, revert the new creation modules, tests, exports, and any shared-helper additions together while preserving the already-complete Phase 1-3 functionality. If one creation family is problematic, remove or disable that family while keeping the remaining Phase 4 work isolated behind package exports.

## Success Criteria
- The library exposes Spark-native `MathFeatures`, `RelativeFeatures`, and `CyclicalFeatures` classes through the package root and creation subpackage.
- All new behavior executes through native PySpark operations only, with no `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes in representative plans.
- Tests written before implementation cover happy paths, lifecycle behavior, variable/reference validation, naming rules, null and error handling, drop-original behavior, learned `max_values_` state for cyclical features, and non-selected-column stability.
- `pytest`, `mypy src tests`, and `python -m build` remain viable local verification commands after Phase 4 is implemented.
