# Design: Phase 4 Feature Creation

## Technical Approach
Add a new `creation` package to `spark_feature_engine` with three public classes:

- `MathFeatures` for row-wise numeric aggregates
- `RelativeFeatures` for arithmetic features against reference columns
- `CyclicalFeatures` and `CyclicalFeaturesModel` for sine/cosine projections with learned or configured maxima

The implementation will reuse the existing Spark ML base classes and validation helpers. `MathFeatures` and `RelativeFeatures` will be direct transformers because they do not need learned state. `CyclicalFeatures` will be an estimator-model pair so the fit step can consistently materialize `variables_` and `max_values_`, even when maximum values are supplied explicitly.

All derived values will be built from Spark-native column expressions and DataFrame projections. No Python UDFs will be used.

## Architecture Decisions

### Decision: Model creation as a dedicated subpackage
**Choice**: Create `src/spark_feature_engine/creation/` with dedicated modules and package exports.

**Alternatives considered**:
- Put creation transformers directly at package root.
- Mix creation helpers into existing encoding or transformation packages.

**Rationale**: The reference library treats creation as its own domain, and a dedicated subpackage keeps public APIs, tests, and future additions like geo or tree-based creators isolated.

### Decision: Use transformers for stateless creation and an estimator-model pair for cyclical features
**Choice**: Implement `MathFeatures` and `RelativeFeatures` as `BaseSparkTransformer` subclasses, and implement `CyclicalFeatures.fit(df) -> CyclicalFeaturesModel`.

**Alternatives considered**:
- Make all three plain transformers.
- Make all three estimators for uniformity.

**Rationale**: Only cyclical features can learn state from data. Keeping the other classes stateless matches the repository's current patterns and avoids unnecessary fitted wrappers.

### Decision: Support a constrained Spark-native function surface for math features
**Choice**: Support `sum`, `mean`, `min`, `max`, and `prod` for `MathFeatures`, with optional explicit `new_variable_names` and deterministic generated names when names are omitted.

**Alternatives considered**:
- Attempt arbitrary pandas-like aggregation parity.
- Limit Phase 4 to only one aggregate such as `sum`.

**Rationale**: Arbitrary pandas aggregations do not translate cleanly to native Spark row-wise expressions. This subset covers the common use cases while remaining straightforward to implement and test without UDFs.

### Decision: Preserve reference-style arithmetic coverage for relative features
**Choice**: Support `add`, `sub`, `mul`, `div`, `truediv`, `floordiv`, `mod`, and `pow`, with generated names `<variable>_<function>_<reference>` and `fill_value` controlling division-by-zero behavior.

**Alternatives considered**:
- Limit relative features to only add, subtract, multiply, and divide.
- Reject zero denominators unconditionally.

**Rationale**: The full arithmetic set is still Spark-native and keeps the Phase 4 API close to the reference package. `fill_value` preserves a useful safety valve for division-like operations without needing UDFs.

### Decision: Reuse central validation helpers instead of adding a creation base class
**Choice**: Extend `src/spark_feature_engine/_validation.py` with creation-specific validators and keep module-local expression builders near each transformer.

**Alternatives considered**:
- Create a new shared `BaseCreation` abstraction.
- Duplicate validation logic inside each creation module.

**Rationale**: The current codebase already centralizes cross-domain validation in `_validation.py`. Adding a new inheritance layer would increase surface area before the domain proves it needs one.

## Data Flow

### MathFeatures
1. Resolve numeric variables, defaulting to all numeric columns when `variables` is unset.
2. Validate there are at least two resolved variables.
3. Normalize configured functions and output names.
4. Build one Spark expression per generated feature.
5. Return a projected DataFrame with original columns plus generated columns, or drop selected source columns when configured.

### RelativeFeatures
1. Resolve numeric `reference` columns and selected `variables` columns.
2. If `variables` is unset, default to all numeric columns not present in `reference`.
3. Validate supported arithmetic functions, output-name collisions, and division settings.
4. For each function/reference/variable combination, build a Spark expression.
5. For division-like functions, pre-check zero denominators when `fill_value` is `None`; otherwise emit `when(reference == 0, fill_value)` guarded expressions.
6. Return a projected DataFrame with generated columns appended or with source/reference columns dropped when configured.

### CyclicalFeatures
1. Resolve numeric variables.
2. During fit, either validate the configured `max_values` mapping or learn maxima with a single aggregate query.
3. Persist `variables_` and `max_values_` on the fitted model.
4. During transform, validate source columns and output-name collisions.
5. Build `<variable>_sin` and `<variable>_cos` expressions using `sin(variable * 2π / max_value)` and `cos(variable * 2π / max_value)`.
6. Return a projected DataFrame with generated columns appended or selected source columns dropped when configured.

## File Changes

### New files
- `src/spark_feature_engine/creation/__init__.py`
- `src/spark_feature_engine/creation/math_features.py`
- `src/spark_feature_engine/creation/relative_features.py`
- `src/spark_feature_engine/creation/cyclical_features.py`
- `tests/creation/test_math_features.py`
- `tests/creation/test_relative_features.py`
- `tests/creation/test_cyclical_features.py`
- `tests/test_phase4_validation_helpers.py`

### Modified files
- `src/spark_feature_engine/__init__.py`
- `src/spark_feature_engine/_validation.py`
- `tests/test_base_transformer.py`

## Interfaces / Contracts

### `MathFeatures`
- Base: `BaseSparkTransformer`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `func: str | Sequence[str]`
  - `new_variable_names: Sequence[str] | None = None`
  - `drop_original: bool = False`
- Output contract:
  - Adds one derived column per configured function.
  - Default generated names follow `{function}_{var1}_{var2}_...`.

### `RelativeFeatures`
- Base: `BaseSparkTransformer`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `reference: Sequence[str]`
  - `func: Sequence[str]`
  - `fill_value: float | None = None`
  - `drop_original: bool = False`
- Output contract:
  - Adds one derived column per valid variable/reference/function combination.
  - Generated names follow `{variable}_{function}_{reference}`.
  - For `div`, `truediv`, `floordiv`, and `mod`, zero denominators raise unless `fill_value` is configured.

### `CyclicalFeatures`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `max_values: Mapping[str, float] | None = None`
  - `drop_original: bool = False`
- Output contract:
  - `fit()` returns `CyclicalFeaturesModel`.
  - The fitted model exposes `variables_` and `max_values_`.
  - Transform adds `{variable}_sin` and `{variable}_cos` columns.

## Testing Strategy
- Add creation-focused helper tests first for function normalization, variable/reference resolution, max-value validation, output-name collision checks, and learned trailing-underscore state.
- Write feature tests before implementation for each creation class.
- Cover:
  - happy-path transforms
  - explicit variable selection and default resolution
  - drop-original behavior
  - deterministic generated column names
  - invalid functions, invalid column types, and duplicate names
  - zero-denominator behavior for relative features
  - learned versus configured maxima for cyclical features
  - non-selected-column stability
  - Spark plan assertions showing no Python execution nodes
- Run targeted pytest modules during development, then full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`.

## Migration / Rollout
- Add the new creation subpackage and exports without changing existing Phase 1-3 behavior.
- Keep the phase isolated so future Phase 4 extensions like geo or tree-based creators can be added independently.
- Verify representative Spark plans before considering the phase complete.

## Open Questions
- None for the approved Phase 4 slice.
