# Selection Specification

## Requirements

### Requirement: DropFeatures
The library MUST provide a configured selector that removes explicitly named features from a Spark DataFrame.

#### Scenario: Configured features are removed
- GIVEN a dataset and a configured list of feature names to drop
- WHEN `DropFeatures` is applied
- THEN the transformed dataset SHALL exclude those features
- AND all retained columns SHALL preserve their original values and row order

#### Scenario: Dropping all columns is rejected
- GIVEN a `DropFeatures` configuration that would remove every column in the dataset
- WHEN the transformer is fit or applied
- THEN the operation SHALL fail with a validation error

### Requirement: DropHighPSIFeatures
The library MUST provide a learned selector that drops features whose distribution differs across classification target values beyond a configured PSI threshold.

#### Scenario: Features with high target-class PSI are removed
- GIVEN a binary classification dataset with one or more features whose class-conditional distributions differ materially between target classes
- WHEN `DropHighPSIFeatures` is fit and then applied
- THEN the fitted selector SHALL expose per-feature PSI values and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude the learned drop set

#### Scenario: PSI target comparison requires binary classification labels
- GIVEN a dataset whose target column is not binary
- WHEN the user attempts to fit `DropHighPSIFeatures`
- THEN the fit SHALL fail with a validation error

### Requirement: SelectByInformationValue
The library MUST provide a learned selector that keeps features whose information value against a binary classification target meets a configured threshold.

#### Scenario: Features above the information-value threshold are retained
- GIVEN a binary classification dataset with categorical and or numerical candidate features
- WHEN `SelectByInformationValue` is fit and then applied
- THEN the fitted selector SHALL expose per-feature information values and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL retain only features whose information value meets the configured threshold

#### Scenario: Numerical features are binned before information-value evaluation
- GIVEN a dataset with numerical variables selected for information-value selection
- WHEN `SelectByInformationValue` is fit
- THEN the selector SHALL evaluate those variables through approved Spark-native binning logic before calculating information value

### Requirement: SelectByTargetMeanPerformance
The library MUST provide a learned selector that evaluates each feature independently through target-mean encoding against a binary classification target and keeps features whose validation performance meets a threshold.

#### Scenario: Features with sufficient target-mean performance are retained
- GIVEN a binary classification dataset and a valid performance metric
- WHEN `SelectByTargetMeanPerformance` is fit and then applied
- THEN the fitted selector SHALL expose per-feature validation performance and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL retain only features whose target-mean-based validation performance meets the configured threshold

### Requirement: SelectBySingleFeaturePerformance
The library MUST provide a learned selector that trains a classifier on each selected feature independently and keeps features whose validation performance meets a threshold.

#### Scenario: Features with weak individual predictive performance are dropped
- GIVEN a binary classification dataset, a supported native classifier, and multiple selected features
- WHEN `SelectBySingleFeaturePerformance` is fit and then applied
- THEN the fitted selector SHALL expose per-feature validation performance and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude features below the configured threshold

### Requirement: SelectByShuffling
The library MUST provide a learned selector that measures performance drift after shuffling each feature and drops features whose drift is below a configured threshold.

#### Scenario: Features with low shuffle importance are dropped
- GIVEN a binary classification dataset, a supported native classifier, and selected features
- WHEN `SelectByShuffling` is fit and then applied
- THEN the fitted selector SHALL expose baseline performance, per-feature performance drifts, and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude features whose performance drift does not meet the threshold

#### Scenario: Shuffle selection is reproducible with a fixed random state
- GIVEN identical data, configuration, estimator, and `random_state`
- WHEN `SelectByShuffling` is fit repeatedly
- THEN the learned performance drifts and dropped features SHALL be deterministic

### Requirement: ProbeFeatureSelection
The library MUST provide a learned selector that compares real features against generated probe features in collective mode and drops features that do not beat the configured probe threshold.

#### Scenario: Features weaker than probe features are dropped
- GIVEN a binary classification dataset and a supported native classifier with feature-importance extraction support
- WHEN `ProbeFeatureSelection` is fit and then applied
- THEN the fitted selector SHALL expose the probe-feature threshold, feature scores, and learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude features that do not beat the probe threshold

### Requirement: RecursiveFeatureAddition
The library MUST provide a learned selector that recursively adds features in ranked order and keeps only features whose incremental contribution exceeds a configured threshold.

#### Scenario: Recursive addition keeps only incrementally useful features
- GIVEN a binary classification dataset and a supported native classifier with feature-importance extraction support
- WHEN `RecursiveFeatureAddition` is fit and then applied
- THEN the fitted selector SHALL expose ranked feature importance metadata, incremental performance drifts, and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL retain only features accepted by the recursive-addition rule

### Requirement: RecursiveFeatureElimination
The library MUST provide a learned selector that recursively removes low-ranked features and keeps only features whose removal would degrade performance beyond a configured threshold.

#### Scenario: Recursive elimination removes dispensable features
- GIVEN a binary classification dataset and a supported native classifier with feature-importance extraction support
- WHEN `RecursiveFeatureElimination` is fit and then applied
- THEN the fitted selector SHALL expose ranked feature importance metadata, per-feature removal drifts, and the learned features to drop through trailing-underscore learned attributes
- AND the transformed dataset SHALL exclude features removable under the recursive-elimination rule

### Requirement: Native Classifier Compatibility
Phase 6 model-based selectors MUST work with Spark-DataFrame-native binary classifiers that satisfy the project's selector compatibility contract.

#### Scenario: Unsupported estimators are rejected early
- GIVEN a selector configured with an estimator that cannot train, score, or expose required outputs on Spark DataFrames under the selector contract
- WHEN the user attempts to fit a Phase 6 model-based selector
- THEN the fit SHALL fail with a validation error instead of silently falling back to local execution

### Requirement: Phase 6 Validation and Lifecycle
Phase 6 selectors MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes where learning occurs, and MUST transform datasets by removing only configured or learned features.

#### Scenario: Invalid configuration or schema is rejected
- GIVEN a selector configured with missing columns, duplicate variables, invalid thresholds, unsupported metrics, unsupported distributions, non-binary targets, incompatible estimators, or a configuration that would produce an empty output
- WHEN the user attempts to fit or transform data with that selector
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN a Phase 6 selector is fit and then applied to the selected columns
- THEN only configured or learned dropped features SHALL be removed from the transformed result
- AND all retained columns SHALL preserve their original values

### Requirement: Native Spark Selection Execution
Phase 6 selectors MUST execute through native Spark SQL, DataFrame, or compatible Spark ML operations and MUST NOT require Python UDF execution for fitting, scoring, or transforming learned feature selections.

#### Scenario: Selection plans remain Spark-native
- GIVEN a Phase 6 selector fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` execution nodes
