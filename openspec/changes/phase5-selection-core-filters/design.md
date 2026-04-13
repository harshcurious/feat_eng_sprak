# Design: Phase 5 Core Filter Selection

## Technical Approach
Add a new `selection` package to `spark_feature_engine` with four public learned selectors:

- `DropConstantFeatures` / model
- `DropDuplicateFeatures` / model
- `DropCorrelatedFeatures` / model
- `SmartCorrelatedSelection` / model

Each selector will be implemented as a Spark ML estimator-model pair. The fit step will learn a deterministic `features_to_drop_` set and any supporting artifacts such as correlated groups or duplicate mappings. The transform step will remove only those learned columns.

All feature-analysis logic will use Spark-native aggregations, counts, summary statistics, and DataFrame operations. No Python UDFs will be used.

## Architecture Decisions

### Decision: Model selection as a dedicated subpackage
**Choice**: Create `src/spark_feature_engine/selection/` with dedicated modules and package exports.

**Alternatives considered**:
- Put selectors directly at package root.
- Mix selectors into validation or transformation modules.

**Rationale**: Selection is a distinct domain in the reference library and will likely expand. A dedicated package keeps fit/transform contracts, tests, and future selector families isolated.

### Decision: Use estimator-model pairs for all Phase 5 selectors
**Choice**: Implement each selector as `BaseSparkEstimator -> BaseSparkModel`.

**Alternatives considered**:
- Use plain transformers for the simpler selectors.
- Create one shared monolithic selection estimator.

**Rationale**: All Phase 5 selectors learn a drop set from data. Using a consistent estimator-model contract keeps the API predictable and matches the learned-state pattern used elsewhere in the package.

### Decision: Keep smart correlated selection limited to non-target heuristics
**Choice**: Support only `missing_values`, `cardinality`, and `variance` selection rules in Phase 5.

**Alternatives considered**:
- Include target- or model-based modes now.
- Defer `SmartCorrelatedSelection` entirely.

**Rationale**: The approved scope is core filter selection only. These heuristics remain target-free and Spark-native while still covering the most useful grouped-correlation behavior.

### Decision: Use deterministic drop ordering
**Choice**: Base all learned drop decisions on stable column-order and stable tie-break rules.

**Alternatives considered**:
- Allow Spark aggregation ordering to determine the drop set.
- Use arbitrary set ordering.

**Rationale**: Selection artifacts must be stable across repeated fits on the same dataset. Deterministic ordering is essential for reliable tests and predictable user behavior.

### Decision: Reuse central validation helpers and add selection-specific helpers
**Choice**: Extend `src/spark_feature_engine/_validation.py` with selection-specific validators and keep selector-specific aggregation logic within the new modules.

**Alternatives considered**:
- Add a full `BaseSelector` abstraction immediately.
- Duplicate validation and drop checks per selector.

**Rationale**: The existing package already centralizes cross-domain validation in `_validation.py`. Phase 5 can stay simpler by adding focused helpers before introducing more inheritance.

## Data Flow

### DropConstantFeatures
1. Resolve target columns, defaulting to all columns when `variables` is unset.
2. Validate the threshold and missing-value policy.
3. For each candidate column, compute the predominant-value proportion using grouped counts and row totals.
4. Learn `features_to_drop_` for features whose predominant proportion meets or exceeds the configured threshold.
5. Reject configurations that would remove all selected columns.
6. Transform by dropping the learned feature set.

### DropDuplicateFeatures
1. Resolve target columns.
2. Build deterministic per-column fingerprints from Spark-native expressions over the full dataset.
3. Group columns by identical fingerprints.
4. Keep the first stable representative from each duplicate group and learn the rest in `features_to_drop_`.
5. Transform by dropping the learned duplicate columns.

### DropCorrelatedFeatures
1. Resolve numeric target columns and validate at least two are present.
2. Materialize a vectorized numeric view or pairwise correlation-ready frame with Spark-native APIs.
3. Compute pairwise correlations and collect only the small correlation summary needed to choose drops.
4. Apply a deterministic rule to each correlated pair and learn `correlated_feature_sets_` plus `features_to_drop_`.
5. Transform by dropping the learned features.

### SmartCorrelatedSelection
1. Reuse the correlated-group discovery flow from `DropCorrelatedFeatures`.
2. Build correlated groups above the threshold.
3. Score columns within each group by the configured non-target rule:
   - fewer missing values
   - lower or higher cardinality, depending on approved reference behavior
   - higher variance
4. Keep one representative per group using deterministic tie-breaks.
5. Learn `selected_features_`, `correlated_feature_sets_`, and `features_to_drop_`.
6. Transform by dropping the learned features.

## File Changes

### New files
- `src/spark_feature_engine/selection/__init__.py`
- `src/spark_feature_engine/selection/drop_constant_features.py`
- `src/spark_feature_engine/selection/drop_duplicate_features.py`
- `src/spark_feature_engine/selection/drop_correlated_features.py`
- `src/spark_feature_engine/selection/smart_correlated_selection.py`
- `tests/selection/test_drop_constant_features.py`
- `tests/selection/test_drop_duplicate_features.py`
- `tests/selection/test_drop_correlated_features.py`
- `tests/selection/test_smart_correlated_selection.py`
- `tests/test_phase5_validation_helpers.py`

### Modified files
- `src/spark_feature_engine/__init__.py`
- `src/spark_feature_engine/_validation.py`
- `src/spark_feature_engine/base.py` (only if selector lifecycle support is needed)
- `tests/test_base_transformer.py`
- `tests/test_package_setup.py`

## Interfaces / Contracts

### `DropConstantFeatures`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `tol: float = 1.0`
  - `missing_values: str = "raise" | "ignore" | "include"`
- Learned attributes:
  - `variables_`
  - `features_to_drop_`
  - optional metadata for predominant-value scores

### `DropDuplicateFeatures`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
- Learned attributes:
  - `variables_`
  - `features_to_drop_`
  - `duplicated_feature_groups_`

### `DropCorrelatedFeatures`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `method: str = "pearson"` or approved Spark-native correlation methods
  - `threshold: float`
- Learned attributes:
  - `variables_`
  - `features_to_drop_`
  - `correlated_feature_sets_`

### `SmartCorrelatedSelection`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `threshold: float`
  - `selection_method: str` in `{"missing_values", "cardinality", "variance"}`
- Learned attributes:
  - `variables_`
  - `selected_features_`
  - `features_to_drop_`
  - `correlated_feature_sets_`

## Testing Strategy
- Add selection-focused helper tests first for threshold validation, supported selection methods, minimum-variable checks, deterministic drop protection, and learned trailing-underscore state.
- Write selector tests before implementation for each Phase 5 selector.
- Cover:
  - happy-path fits and transforms
  - default variable resolution and explicit variable selection
  - learned drop lists and correlated group metadata
  - deterministic repeated fits
  - invalid thresholds, unsupported methods, and invalid column types
  - empty-output prevention
  - non-selected-column stability
  - Spark plan assertions showing no Python execution nodes
- Run targeted pytest modules during development, then full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`.

## Migration / Rollout
- Add the new selection subpackage and exports without changing existing Phases 1-4 behavior.
- Keep Phase 6 creation-family decisions deferred and out of this change.
- Verify representative Spark plans before considering the phase complete.

## Open Questions
- None for the approved Phase 5 slice.
