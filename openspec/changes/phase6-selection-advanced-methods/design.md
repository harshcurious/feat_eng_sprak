# Design: Phase 6 Advanced Feature Selection

## Technical Approach
Add the remaining approved feature-selection methods to `src/spark_feature_engine/selection/` while preserving the project's Spark-first lifecycle. Phase 6 splits naturally into three families:

- **Configured dropper**: `DropFeatures`
- **Target-aware statistical selectors**: `DropHighPSIFeatures`, `SelectByInformationValue`, `SelectByTargetMeanPerformance`
- **Model-based selectors**: `SelectBySingleFeaturePerformance`, `SelectByShuffling`, `ProbeFeatureSelection`, `RecursiveFeatureAddition`, `RecursiveFeatureElimination`

Most selectors in this phase learn metadata from the training DataFrame and therefore should use estimator-model pairs. `DropFeatures` should stay a transformer because it does not learn from data and would gain nothing from a fit stage.

## Architecture Decisions

### Decision: Keep `DropFeatures` as a direct transformer
**Choice**: Implement `DropFeatures` as a validated configured transformer instead of an estimator-model pair.

**Alternatives considered**:
- Force `DropFeatures` into an estimator-model lifecycle for surface consistency.
- Omit `DropFeatures` from this phase.

**Rationale**: `DropFeatures` is deterministic configuration, not learned state. A direct transformer keeps the API honest and simpler while remaining compatible with Spark ML pipelines.

### Decision: Introduce a selector-estimator compatibility layer
**Choice**: Add an internal helper layer that validates estimator capabilities and centralizes fit / score / importance extraction for Spark-DataFrame-native classifiers.

**Alternatives considered**:
- Hard-code support for Spark ML only.
- Let each selector perform its own estimator introspection.

**Rationale**: Multiple selectors need the same operations: fit on a Spark DataFrame, produce scored predictions, and sometimes expose feature importance. Centralizing this avoids repeated fragile logic and leaves room for compatible libraries such as Spark-native XGBoost wrappers.

### Decision: Classification-only scope for supervised selectors
**Choice**: Restrict all target-aware and model-based selectors in Phase 6 to binary classification.

**Alternatives considered**:
- Support regression at launch.
- Support both binary and multiclass classification.

**Rationale**: The approved scope is classification first. Binary-target contracts are narrower, easier to validate, and match the immediate need for PSI, IV, and probability-based scoring.

### Decision: PSI compares feature distributions across target classes
**Choice**: Define `DropHighPSIFeatures` as comparing each feature's distribution between target class 0 and target class 1.

**Alternatives considered**:
- Compare two arbitrary datasets.
- Compare one dataset split by a user-provided split column.

**Rationale**: This matches the approved Phase 6 direction and keeps PSI target-aware within a single binary classification dataset.

### Decision: Use deterministic fold assignment and tie-breaking
**Choice**: Use explicit fold assignment, stable column ordering, and stable metric tie-breaks across all supervised selectors.

**Alternatives considered**:
- Depend on implicit Spark partition order or estimator internals.
- Allow arbitrary ordering when scores tie.

**Rationale**: Phase 6 adds heavy learned-state behavior that must be reproducible for tests and user trust.

### Decision: Prefer metric evaluation through scored prediction DataFrames
**Choice**: Compute selector metrics from Spark prediction outputs rather than local metric libraries.

**Alternatives considered**:
- Collect predictions locally and score in Python.
- Require every estimator to provide its own bespoke evaluator.

**Rationale**: Prediction-output evaluation keeps execution Spark-native and lets the selector layer reuse Spark ML evaluators or equivalent DataFrame-native metrics.

### Decision: Ship `ProbeFeatureSelection` in collective mode first
**Choice**: Support only collective probe selection in the first Spark-native release.

**Alternatives considered**:
- Support both collective and single-feature-performance probe modes immediately.
- Ship the single-feature probe mode first.

**Rationale**: Collective mode has a narrower compatibility surface and aligns better with a feature-importance-based Spark-native estimator contract. It reduces Phase 6 scope while keeping the most representative probe-selection workflow.

## Data Flow

### DropFeatures
1. Validate configured `features_to_drop` against the input schema.
2. Reject duplicates, unknown columns, or empty-output outcomes.
3. Transform by dropping the configured columns.

### DropHighPSIFeatures
1. Resolve candidate columns and validate a binary target column.
2. For numerical variables, discretize with approved Spark-native binning.
3. For categorical variables, treat categories as buckets directly.
4. Build per-target-class bucket distributions and compute PSI per feature.
5. Learn `psi_values_`, `variables_`, and `features_to_drop_`.
6. Transform by dropping the learned features.

### SelectByInformationValue
1. Resolve candidate columns and validate a binary target column.
2. Discretize numerical columns with Spark-native binning; keep categorical columns as categories.
3. Build class-conditioned event and non-event rates per bucket.
4. Compute WoE-style components and information value per feature.
5. Learn `information_values_`, `variables_`, and `features_to_drop_`.
6. Transform by dropping the learned features.

### SelectByTargetMeanPerformance
1. Resolve candidate columns and validate a binary target column.
2. Build deterministic folds.
3. Per fold and per feature, learn target-mean encodings on the training split only.
4. Apply the encoding to the validation split and score the single encoded feature against the configured classification metric.
5. Aggregate per-feature validation scores across folds.
6. Learn `feature_performance_`, optional std metadata, and `features_to_drop_`.

### Shared model-based selector flow
1. Resolve numeric candidate columns and validate a binary target column.
2. Validate the estimator against the selector compatibility layer.
3. Build deterministic folds.
4. Train and score candidate feature sets on Spark DataFrames only.
5. Aggregate fold metrics and any feature-importance metadata.
6. Learn `features_to_drop_` and selector-specific metadata.
7. Transform by dropping the learned features.

### SelectBySingleFeaturePerformance
- Score one classifier per feature across folds.
- Retain features whose mean validation score meets the threshold.

### SelectByShuffling
- Train a baseline classifier on all selected features per fold.
- Shuffle one feature at a time within the validation slice using deterministic randomness.
- Re-score and measure performance drift.
- Drop features whose drift is below the threshold.

### ProbeFeatureSelection
- Generate deterministic probe features natively with `rand`, `randn`, bucketization, or other Spark expressions.
- Score real and probe features collectively from estimator importance only.
- Derive a threshold from probe statistics and drop weaker real features.

### RecursiveFeatureAddition
- Rank features from a baseline model's importance metadata.
- Train incrementally larger feature sets in rank order.
- Keep a feature only if the metric improvement exceeds the threshold.

### RecursiveFeatureElimination
- Rank features from a baseline model's importance metadata.
- Remove low-ranked features one at a time.
- Keep the feature removed when performance degradation stays within threshold; otherwise retain it.

## File Changes

### New files
- `openspec/changes/phase6-selection-advanced-methods/proposal.md`
- `openspec/changes/phase6-selection-advanced-methods/specs/selection/spec.md`
- `openspec/changes/phase6-selection-advanced-methods/design.md`
- `openspec/changes/phase6-selection-advanced-methods/tasks.md`
- `src/spark_feature_engine/selection/drop_features.py`
- `src/spark_feature_engine/selection/drop_psi_features.py`
- `src/spark_feature_engine/selection/information_value.py`
- `src/spark_feature_engine/selection/target_mean_selection.py`
- `src/spark_feature_engine/selection/single_feature_performance.py`
- `src/spark_feature_engine/selection/shuffle_features.py`
- `src/spark_feature_engine/selection/probe_feature_selection.py`
- `src/spark_feature_engine/selection/recursive_feature_addition.py`
- `src/spark_feature_engine/selection/recursive_feature_elimination.py`
- Internal selection helpers for estimator compatibility, fold assignment, metrics, and probe generation as needed
- Matching tests under `tests/selection/`

### Modified files
- `src/spark_feature_engine/selection/__init__.py`
- `src/spark_feature_engine/__init__.py`
- `src/spark_feature_engine/_validation.py`
- `src/spark_feature_engine/base.py` only if shared selector lifecycle support is needed
- `tests/test_base_transformer.py`
- `tests/test_package_setup.py`

## Interfaces / Contracts

### `DropFeatures`
- Base: direct transformer
- Inputs:
  - `features_to_drop: Sequence[str]`
- Learned attributes:
  - none required beyond validated configured state

### `DropHighPSIFeatures`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `target: str`
  - `threshold: float | str`
  - `bins: int`
  - `strategy: str`
  - `missing_values: str`
- Learned attributes:
  - `variables_`
  - `psi_values_`
  - `features_to_drop_`

### `SelectByInformationValue`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `target: str`
  - `bins: int`
  - `strategy: str`
  - `threshold: float`
- Learned attributes:
  - `variables_`
  - `information_values_`
  - `features_to_drop_`

### `SelectByTargetMeanPerformance`
- Base: `BaseSparkEstimator`
- Inputs:
  - `variables: Sequence[str] | None = None`
  - `target: str`
  - `bins: int`
  - `strategy: str`
  - `scoring: str`
  - `threshold: float | None`
  - `cv: int`
- Learned attributes:
  - `variables_`
  - `feature_performance_`
  - `feature_performance_std_`
  - `features_to_drop_`

### Model-based selectors
- Base: `BaseSparkEstimator`
- Shared inputs:
  - `variables: Sequence[str] | None = None`
  - `target: str`
  - `estimator: object`
  - `scoring: str`
  - `cv: int`
  - selector-specific thresholds and `random_state` where applicable
- Shared learned attributes:
  - `variables_`
  - `features_to_drop_`
  - selector-specific score / drift / importance metadata

### Estimator compatibility contract
Supported estimators should:
- fit on Spark DataFrames without local conversion
- produce prediction outputs needed for the configured classification metric
- expose feature importance or coefficients for recursive and collective probe selectors, when those selectors require ranking
- respect deterministic configuration such as `seed` or equivalent when randomness is involved

## Testing Strategy
- Add Phase 6 helper tests first for:
  - binary-target validation
  - supported scoring options
  - estimator compatibility checks
  - deterministic fold assignment
  - empty-output protections
  - deterministic random-state behavior for shuffle and probes
- Write selector tests before implementation for each new Phase 6 selector.
- Cover:
  - happy-path fits and transforms
  - classification-only validation failures
  - target-aware binning and bucket behavior
  - per-feature learned metrics and drift metadata
  - deterministic repeated fits
  - unsupported estimator rejection
  - non-selected-column stability
  - representative native-plan assertions
- Run targeted pytest modules during development, then full verification with `uv run pytest`, `uv run mypy src tests`, and `uv run python -m build`.

## Migration / Rollout
- Add the new selectors without changing existing Phase 1-5 behavior.
- Keep MRMR explicitly deferred beyond Phase 6.
- Delay any regression or multiclass expansion to a later phase.

## Open Questions
- None for the approved Phase 6 slice.
