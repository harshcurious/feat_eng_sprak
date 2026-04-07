# Proposal: Phase 1 Foundation and Imputation

## Intent
Establish the project foundation for `spark-feature-engine` and deliver the first PySpark-native missing-data transformers. This phase creates the package, tooling, test harness, and shared transformer patterns needed to mirror `feature_engine` behavior while staying aligned with PySpark ML conventions.

## Scope
### In Scope
- Project bootstrap for a typed, testable Python package, including `pyproject.toml`, Ruff configuration, mypy support, pytest setup, and a reusable local `SparkSession` fixture.
- Initial source and test directory structure for a PySpark-native feature engineering library.
- A shared `BaseSparkTransformer` abstraction built on `pyspark.ml.Transformer` and `Params` to centralize parameter handling, variable validation, and fitted-attribute conventions.
- Imputation support for the first missing-data use cases:
  - `MeanMedianImputer` for numerical columns using mean or median statistics.
  - `ArbitraryNumberImputer` for numerical columns using user-provided replacement values.
  - `CategoricalImputer` for categorical columns using a configured fill value or a default of `"missing"`.
  - `DropMissingData` for row removal based on null presence in selected columns.
- Test-first coverage for transformer behavior, parameter validation, fit/transform lifecycle, and native Spark SQL execution paths.

### Out of Scope
- Any non-imputation transformers, including encoding, discretisation, outlier handling, scaling, or feature selection.
- Full parity with all `feature_engine` APIs beyond the Phase 1 transformer set.
- Pipeline persistence, model serialization hardening, or backward-compatibility guarantees across library versions.
- Performance tuning beyond choosing native Spark SQL operations and avoiding UDFs.
- CI workflow setup, packaging publication, or release automation.

## Approach
Create a minimal but production-oriented project skeleton, then define a common transformer base that codifies PySpark ML parameter patterns and fitted-state expectations. Build the imputation transformers against that foundation using native DataFrame operations only, with pytest-driven tests proving null handling, statistics computation, selected-column behavior, and trailing-underscore fitted attributes. Align public APIs with type hints and repository tooling so future phases can add more transformers without reworking core conventions.

## Affected Areas
- `pyproject.toml` and repository-level tool configuration.
- `src/spark_feature_engine/` package structure and base transformer module(s).
- `src/spark_feature_engine/imputation/` transformer implementations.
- `tests/` suite, including Spark fixture setup and imputer/base-class coverage.
- Local developer quality gates for Ruff format, Ruff lint, mypy, and pytest.

## Risks
- PySpark ML distinguishes between estimators and transformers; forcing all shared behavior into a single base abstraction may create lifecycle inconsistencies if fitted and stateless components are not separated cleanly.
- Median imputation in Spark can require approximate or aggregation-specific strategies; proposal and later specs must define expected behavior clearly to avoid divergence from `feature_engine` semantics.
- Type validation and variable selection rules can become brittle if they assume pandas-style behavior instead of Spark schema realities.
- Early project structure decisions will propagate into later phases, so misplaced package boundaries could increase refactoring cost.

## Rollback Plan
If Phase 1 proves unstable, revert the new project bootstrap and imputation modules as a single change set, returning the repository to its current OpenSpec-only state. If only specific transformers are problematic, disable or remove the affected modules and tests while preserving the project foundation and base abstractions that remain valid for later phases.

## Success Criteria
- The repository contains a working Python package skeleton with configured formatting, linting, typing, and pytest test support.
- A reusable base transformer pattern exists for PySpark ML components, including validated Params usage and trailing-underscore fitted attributes.
- The four Phase 1 missing-data transformers are implemented with zero UDF usage and native Spark SQL/DataFrame operations only.
- Tests demonstrate correct null handling, parameter validation, fit/transform behavior, and column-scoped operation for the in-scope transformers.
- Ruff format, Ruff lint, mypy, and pytest can be used as the expected local quality gates for this phase.
