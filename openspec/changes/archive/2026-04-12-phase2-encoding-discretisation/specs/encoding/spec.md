# Encoding Specification

## Requirements

### Requirement: One-Hot Encoding
The library MUST provide a fitted one-hot encoder that learns the categories to expand for each selected variable and replaces each selected variable with deterministic binary output columns for those learned categories.

#### Scenario: Learned categories expand into binary columns
- GIVEN a dataset with selected categorical variables and untouched non-selected columns
- WHEN a one-hot encoder is fit on the dataset and its fitted model transforms that dataset
- THEN the transformed dataset SHALL contain one generated binary column per learned category for each selected variable
- AND the original selected variables SHALL be removed from the transformed dataset
- AND non-selected columns SHALL remain unchanged

#### Scenario: Unseen categories are ignored without Python fallback logic
- GIVEN a fitted one-hot encoder model and transform-time rows containing categories not observed during fit
- WHEN the model transforms those rows
- THEN the model SHALL leave all generated columns for that variable at 0 for the unseen category rows

#### Scenario: Generated output names are deterministic and validated
- GIVEN a one-hot encoder configuration whose generated output names would collide with each other or with existing dataset columns
- WHEN the user attempts to fit or transform data
- THEN the operation SHALL fail with a validation error instead of silently overwriting columns

### Requirement: Ordinal Encoding
The library MUST provide a fitted ordinal encoder that learns a deterministic integer mapping for each selected categorical variable and replaces observed categories with those learned codes.

#### Scenario: Selected variables are replaced by learned ordinal codes
- GIVEN a dataset with selected categorical variables and untouched non-selected columns
- WHEN an ordinal encoder is fit and then used to transform the dataset
- THEN each selected variable SHALL be replaced with integer codes derived from the learned category mapping for that variable
- AND non-selected columns SHALL remain unchanged

#### Scenario: Learned mappings are deterministic across fits on the same data
- GIVEN the same fit dataset and the same ordinal encoder configuration
- WHEN the encoder is fit multiple times
- THEN the learned category-to-code mapping SHALL be stable for each selected variable

#### Scenario: Unseen categories follow the configured policy
- GIVEN a fitted ordinal encoder model and transform-time rows containing categories not observed during fit
- WHEN the encoder is configured to ignore, encode, or raise for unseen categories and the model transforms those rows
- THEN the transformed result SHALL respectively preserve a null-like missing output, emit a reserved unseen code, or fail with a validation error

### Requirement: Count and Frequency Encoding
The library MUST provide a fitted encoder that learns either per-category counts or per-category frequencies for each selected categorical variable and replaces those categories with the learned numeric values.

#### Scenario: Count encoding uses category occurrence totals
- GIVEN a dataset with repeated categories in selected variables
- WHEN a count-frequency encoder configured for count mode is fit and then used to transform the dataset
- THEN each selected category value SHALL be replaced with the learned occurrence count for that category in the fit dataset

#### Scenario: Frequency encoding uses category proportions
- GIVEN a dataset with repeated categories in selected variables
- WHEN a count-frequency encoder configured for frequency mode is fit and then used to transform the dataset
- THEN each selected category value SHALL be replaced with the learned fraction of rows represented by that category in the fit dataset

#### Scenario: Unseen categories follow the configured policy
- GIVEN a fitted count-frequency encoder model and transform-time rows containing categories not observed during fit
- WHEN the encoder is configured to ignore, encode, or raise for unseen categories and the model transforms those rows
- THEN the transformed result SHALL respectively preserve a null-like missing output, emit 0, or fail with a validation error

### Requirement: Rare Label Encoding
The library MUST provide a fitted rare-label encoder that learns which categories are frequent for each selected variable and replaces categories outside that learned frequent set with a configured replacement label.

#### Scenario: Infrequent categories are grouped under the replacement label
- GIVEN a dataset with selected categorical variables containing categories below the configured frequency threshold
- WHEN a rare-label encoder is fit and then used to transform the dataset
- THEN the learned frequent categories for each selected variable SHALL remain unchanged
- AND categories outside the learned frequent set SHALL be replaced with the configured rare label

#### Scenario: Variables below the minimum-category threshold are preserved
- GIVEN a selected variable whose distinct-category count does not exceed the configured minimum-category threshold for rare-label grouping
- WHEN a rare-label encoder is fit and then used to transform the dataset
- THEN that variable SHALL keep all of its observed categories unchanged

#### Scenario: Maximum frequent-category cap is enforced
- GIVEN a rare-label encoder configured with a maximum number of frequent categories
- WHEN fitting identifies more frequent categories than the configured cap
- THEN only the allowed number of most frequent categories SHALL remain explicit
- AND the remaining categories SHALL be grouped under the replacement label

### Requirement: Encoding Validation and Lifecycle
Phase 2 encoding transformers MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes, and MUST affect only the selected variables.

#### Scenario: Invalid target columns or unsupported configuration are rejected
- GIVEN an encoding transformer configured with missing columns, duplicated target columns, incompatible column types, or unsupported parameter values
- WHEN the user attempts to fit or transform data with that transformer
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Learned encoders expose fitted public attributes
- GIVEN an encoder whose fit step derives categories, mappings, or frequent-label sets
- WHEN fitting completes successfully
- THEN the fitted object SHALL expose those learned values through public attribute names that end with an underscore

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN any Phase 2 encoding transformer is applied to the selected columns
- THEN only the selected columns or their generated replacements SHALL differ in the transformed result

### Requirement: Native Spark Encoding Execution
Phase 2 encoding transformers MUST execute through native Spark SQL, DataFrame, or Spark ML operations and MUST NOT require Python UDF execution for fitting or transforming categorical values.

#### Scenario: Encoding plans remain Spark-native
- GIVEN a Phase 2 encoding transformer fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes
