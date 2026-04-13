# Proposal: Phase 3 Outliers and Transformation

## Intent
Extend `spark-feature-engine` with the approved Phase 3 numerical preprocessing primitives for outlier handling and value transformation. This phase adds PySpark-native estimators and transformers that preserve Spark ML lifecycle semantics, Param-based configuration, and zero-UDF execution while reusing the Phase 1/2 foundation and validation helpers.

## Scope
### In Scope
- Outlier handling support for:
  - `Winsorizer`
  - `OutlierTrimmer`
- Value transformation support for:
  - `LogTransformer`
  - `PowerTransformer`
- Test-first development for each feature area, using the existing Spark pytest fixture and repository quality commands.
- Reuse of the established base transformer contract, learned-state conventions, and shared validation helpers.
- PySpark ML `Estimator` / `Transformer` implementations with Param objects where fit-time state is learned.
- Native Spark SQL and DataFrame operations only; no Python UDFs.

## Out of Scope
- Additional outlier detection methods beyond the approved `Winsorizer` and `OutlierTrimmer`.
- Additional transformation families beyond `LogTransformer` and `PowerTransformer`.
- Serialization hardening, performance tuning beyond native Spark expression usage, or broad API redesign.
- CI/release automation changes outside what is needed to support this phase's tests and exports.

## Approach
Implement Phase 3 as two coherent domains: outlier handling and numerical transformation. Learned outlier thresholds should use proper estimator-to-model flows, while stateless transformations should remain direct transformers. The implementation should mirror the practical behavior of the pandas `feature_engine` reference where compatible with Spark semantics, while adapting to distributed DataFrame execution and the existing package architecture. Tests should be written before implementation for each feature area to lock down fit/transform behavior, parameter and schema validation, selected-column scoping, outlier clipping/removal semantics, positive-value constraints for logarithms, exponent handling, and non-selected-column stability.

## Affected Areas
- `src/spark_feature_engine/__init__.py` and package exports for outlier and transformation APIs.
- New modules under `src/spark_feature_engine/outliers/`.
- New modules under `src/spark_feature_engine/transformation/`.
- Possible extension of `src/spark_feature_engine/base.py` and `src/spark_feature_engine/_validation.py` for shared numerical validation, learned-state helpers, and output-shape checks.
- New pytest modules under `tests/outliers/` and `tests/transformation/`.
- Existing package-level import and typing surfaces impacted by new public classes.

## Risks
- Outlier threshold derivation can vary by capping method and may require careful definition to keep Spark-native behavior deterministic.
- `OutlierTrimmer` removes rows, which can affect downstream row counts and may require explicit validation to preserve selected-column semantics.
- `LogTransformer` must reject zero and negative values consistently across fit/transform to match reference behavior and avoid invalid Spark expressions.
- Power transformations on numeric columns must preserve type stability and handle non-selected columns without accidental schema drift.
- Reusing the shared base across fitted and stateless components may require small foundation adjustments to keep lifecycle semantics aligned.

## Rollback Plan
If Phase 3 proves unstable, revert the new outlier and transformation modules, tests, and exports together while preserving the already-complete Phase 1 and Phase 2 work. If only one domain is problematic, remove or disable the affected outlier or transformation family and keep the other Phase 3 additions isolated behind their package exports.

## Success Criteria
- The library exposes the four approved Phase 3 transformers with Param-based public configuration aligned to PySpark ML patterns.
- Learned outlier behavior persists derived thresholds on fitted objects using trailing-underscore attributes.
- All Phase 3 transforms execute through native PySpark operations only, with no Python UDFs.
- Tests written before implementation cover happy paths, parameter/schema validation, fit-versus-transform lifecycle, selected-column scoping, outlier clipping/removal semantics, logarithm positivity checks, exponent behavior, and non-selected-column stability.
- `pytest`, `mypy src tests`, and `python -m build` remain viable local verification commands after Phase 3 is implemented.
