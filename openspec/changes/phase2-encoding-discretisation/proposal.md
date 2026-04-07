# Proposal: Phase 2 Encoding and Discretisation

## Intent
Extend `spark-feature-engine` beyond imputation by adding the next high-value categorical and numerical preprocessing primitives from the approved roadmap. This phase brings PySpark-native encoders and discretisers that reuse the Phase 1 foundation while preserving Spark ML lifecycle semantics, Param-based configuration, and zero-UDF execution.

## Scope
### In Scope
- Categorical encoding support for:
  - `OneHotEncoder`
  - `OrdinalEncoder`
  - `CountFrequencyEncoder`
  - `RareLabelEncoder`
- Numerical discretisation support for:
  - `EqualWidthDiscretiser`
  - `EqualFrequencyDiscretiser`
  - `ArbitraryDiscretiser`
- Test-first development for each feature area, using the existing pytest Spark fixture and local quality commands now available in the repository.
- Reuse and extension of the Phase 1 foundation, including `BaseSparkTransformer`, shared validation helpers, and the established package/export layout.
- PySpark ML `Estimator` / `Transformer` implementations with Param objects and learned trailing-underscore attributes where fitting derives state.
- Native Spark SQL and DataFrame operations only, including joins, aggregations, window-free expressions, bucketizers, and conditional column expressions as needed.

### Out of Scope
- Any target-aware encoding methods such as mean, WoE, or decision-tree encoders.
- Additional discretisers outside the approved scope, including geometric-width or tree-based approaches.
- Broader schema inference redesign, serialization hardening, or backwards-compatibility guarantees beyond the current package baseline.
- Performance optimization beyond choosing Spark-native implementations and avoiding Python UDFs.
- CI/release automation or changes to project tooling outside what is required to support this phase's tests and exports.

## Approach
Implement Phase 2 in two coherent domains: categorical encoding and numerical discretisation. Learned mappings and bin boundaries should use proper estimator-to-model flows when fit-time statistics are required, while purely parameter-driven transforms remain direct transformers. The implementation should mirror the practical behavior of the pandas `feature_engine` reference where it fits Spark semantics, but adapt outputs to distributed DataFrame constraints and the existing Phase 1 architecture. Tests should be written first for each transformer family to lock down fit/transform behavior, validation rules, unseen or infrequent category handling, generated-column conventions, bin-boundary behavior, and non-selected-column stability before implementation begins.

## Affected Areas
- `src/spark_feature_engine/__init__.py` and new package exports for encoding and discretisation APIs.
- New modules under `src/spark_feature_engine/encoding/`.
- New modules under `src/spark_feature_engine/discretisation/`.
- Possible extension of `src/spark_feature_engine/base.py` and `src/spark_feature_engine/_validation.py` for shared categorical/numerical validation and generated-column checks.
- New pytest modules under `tests/encoding/` and `tests/discretisation/`.
- Existing package-level import and typing surfaces impacted by new public classes.

## Risks
- One-hot encoding expands schema width and introduces generated-column naming concerns; poor naming or collision rules could create unstable transforms.
- Spark-native handling of unseen categories, rare-label grouping, and count/frequency lookups depends on joins and conditional logic; incorrect null or unseen handling could diverge from expected behavior.
- Equal-frequency discretisation may require approximate quantile computation in Spark, so the specification must define acceptable learned-boundary behavior clearly.
- Arbitrary discretisation needs explicit behavior for out-of-range values and boundary inclusivity; leaving this implicit would create inconsistent transforms.
- Reusing a transformer-only base across learned and stateless components may require small shared-foundation adjustments to avoid lifecycle mismatches similar to those addressed in Phase 1.

## Rollback Plan
If Phase 2 proves unstable, revert the new encoding and discretisation modules, tests, and exports together while preserving the already-complete Phase 1 foundation and imputation functionality. If only one domain is problematic, remove or disable the affected encoder or discretiser family and keep the other Phase 2 additions isolated behind their package exports.

## Success Criteria
- The library exposes the seven approved Phase 2 transformers with Param-based public configuration aligned to PySpark ML patterns.
- Learned encoders and discretisers persist their derived mappings or bin definitions on fitted objects using trailing-underscore attributes.
- All Phase 2 transforms execute through native PySpark operations only, with no Python UDFs.
- Tests written before implementation cover happy paths, parameter/schema validation, fit-versus-transform lifecycle, selected-column scoping, unseen or infrequent category behavior, and generated output behavior for each feature area.
- `uv run pytest`, `uv run mypy`, and `uv run python -c "import pyspark"` remain viable local verification commands after Phase 2 is implemented.
