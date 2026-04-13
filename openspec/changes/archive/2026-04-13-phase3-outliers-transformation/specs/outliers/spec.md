# Outliers Specification

## Requirements

### Requirement: Winsorizer
The library MUST provide a fitted winsorizer that learns capping thresholds for each selected numerical variable and clips transformed values to those learned bounds.

#### Scenario: Learned caps clip selected values
- GIVEN a dataset with selected numerical variables and untouched non-selected columns
- WHEN a winsorizer is fit on the dataset and its fitted model transforms that dataset
- THEN each selected variable SHALL be clipped to the learned lower and upper bounds for that variable
- AND non-selected columns SHALL remain unchanged

#### Scenario: Learned caps are exposed on the fitted model
- GIVEN a winsorizer whose fit step derives per-variable thresholds from data
- WHEN fitting completes successfully
- THEN the fitted model SHALL expose the learned thresholds through public attribute names that end with an underscore

#### Scenario: Future extreme values remain clipped
- GIVEN a fitted winsorizer model and transform-time rows whose selected values exceed the fit-time learned bounds
- WHEN the model transforms those rows
- THEN the transformed values SHALL be clipped to the learned outer bounds instead of failing because the values are outside the fit-time range

### Requirement: OutlierTrimmer
The library MUST provide an outlier trimmer that removes rows containing outlier values in the selected numerical variables according to learned trimming bounds.

#### Scenario: Rows outside learned bounds are removed
- GIVEN a dataset containing selected numerical variables and rows that fall outside the learned trimming bounds
- WHEN an outlier trimmer is fit on the dataset and its fitted model transforms that dataset
- THEN rows outside the learned bounds for any selected variable SHALL be excluded from the result
- AND rows within the learned bounds SHALL be preserved

#### Scenario: Learned trimming bounds are exposed on the fitted model
- GIVEN an outlier trimmer whose fit step derives per-variable trimming bounds from data
- WHEN fitting completes successfully
- THEN the fitted model SHALL expose the learned bounds through public attribute names that end with an underscore

#### Scenario: Row removal preserves selected-column semantics
- GIVEN a dataset with selected and non-selected columns
- WHEN an outlier trimmer removes rows based on the selected columns
- THEN the resulting dataset SHALL preserve the schema of the remaining rows
- AND non-selected columns SHALL not be altered before row removal

### Requirement: Outlier Validation and Lifecycle
Phase 3 outlier transformers MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes when fitting derives state, and MUST affect only the selected variables.

#### Scenario: Invalid target columns or unsupported configuration are rejected
- GIVEN an outlier transformer configured with missing columns, duplicated target columns, incompatible column types, or unsupported parameter values
- WHEN the user attempts to fit or transform data with that transformer
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN a Phase 3 outlier transformer is applied to the selected columns
- THEN only the selected columns or row membership SHALL differ in the transformed result

### Requirement: Native Spark Outlier Execution
Phase 3 outlier transformers MUST execute through native Spark SQL, DataFrame, or Spark ML operations and MUST NOT require Python UDF execution for fitting or transforming numerical values.

#### Scenario: Outlier plans remain Spark-native
- GIVEN a Phase 3 outlier transformer fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes
