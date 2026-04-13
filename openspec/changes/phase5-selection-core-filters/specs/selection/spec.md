# Selection Specification

## Requirements

### Requirement: DropConstantFeatures
The library MUST provide a learned selector that drops constant and quasi-constant features based on a configured predominant-value threshold using Spark-native operations.

#### Scenario: Constant features are identified and removed
- GIVEN a dataset with selected features where one or more columns contain only a single effective value
- WHEN `DropConstantFeatures` is fit and then applied
- THEN the fitted selector SHALL expose the dropped feature names through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude those dropped features while preserving retained features and row order

#### Scenario: Quasi-constant features are removed by threshold
- GIVEN a dataset with selected features where one column has a predominant value frequency above a configured threshold below `1.0`
- WHEN `DropConstantFeatures` is fit with that threshold
- THEN the selector SHALL mark that feature for removal

### Requirement: DropDuplicateFeatures
The library MUST provide a learned selector that drops exact duplicate features using Spark-native column comparison logic.

#### Scenario: Duplicate columns are removed deterministically
- GIVEN a dataset with selected features where two or more columns contain identical values for all rows
- WHEN `DropDuplicateFeatures` is fit and then applied
- THEN the selector SHALL keep one deterministic representative column from each duplicate set
- AND the transformed dataset SHALL exclude the learned duplicate columns

### Requirement: DropCorrelatedFeatures
The library MUST provide a learned selector that drops numeric features from highly correlated pairs using a configured correlation threshold.

#### Scenario: Correlated numeric pairs cause feature drops
- GIVEN a dataset with selected numeric features where at least one pair has absolute correlation above a configured threshold
- WHEN `DropCorrelatedFeatures` is fit and then applied
- THEN the selector SHALL expose the correlated feature pairs and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude the learned drop set

#### Scenario: Correlation requires numeric selected features
- GIVEN a selector configuration that targets non-numeric features
- WHEN the user attempts to fit `DropCorrelatedFeatures`
- THEN the fit SHALL fail with a validation error instead of coercing unsupported types

### Requirement: SmartCorrelatedSelection
The library MUST provide a learned selector that groups correlated numeric features and keeps one representative per group using non-target filter heuristics only.

#### Scenario: Smart correlated selection keeps one feature per correlated group
- GIVEN a dataset with correlated numeric features and a configured selection method of `missing_values`, `cardinality`, or `variance`
- WHEN `SmartCorrelatedSelection` is fit and then applied
- THEN the selector SHALL expose the learned correlated groups, selected representatives, and dropped features through trailing-underscore learned attributes
- AND the transformed dataset SHALL keep one representative feature per correlated group according to the configured non-target rule

#### Scenario: Unsupported smart selection methods are rejected
- GIVEN a `SmartCorrelatedSelection` configuration outside the approved non-target modes
- WHEN the user attempts to fit the selector
- THEN the fit SHALL fail with a validation error

### Requirement: Selector Validation and Lifecycle
Phase 5 selectors MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes, and MUST transform datasets by removing only the learned features to drop.

#### Scenario: Invalid selector configuration or schema is rejected
- GIVEN a selector configured with missing columns, duplicated variables, incompatible column types, invalid thresholds, unsupported methods, or a configuration that would drop all selected features
- WHEN the user attempts to fit or transform data with that selector
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN a Phase 5 selector is fit and then applied to the selected columns
- THEN only the learned dropped features SHALL be removed from the transformed result
- AND all retained columns SHALL preserve their original values

### Requirement: Native Spark Selection Execution
Phase 5 selectors MUST execute through native Spark SQL, DataFrame, or Spark ML operations and MUST NOT require Python UDF execution for fitting or transforming learned feature selections.

#### Scenario: Selection plans remain Spark-native
- GIVEN a Phase 5 selector fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes
