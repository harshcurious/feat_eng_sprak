# Creation Specification

## Requirements

### Requirement: MathFeatures
The library MUST provide a math feature creator that derives one or more new features from a selected group of numerical variables using supported row-wise aggregate functions expressed through native Spark operations.

#### Scenario: Aggregate features are added from selected variables
- GIVEN a dataset with at least two selected numerical variables and untouched non-selected columns
- WHEN a math feature creator is applied with a supported aggregate function
- THEN the transformed dataset SHALL include the derived feature columns for that function
- AND the original selected variables SHALL remain unchanged unless configured to be dropped
- AND non-selected columns SHALL remain unchanged

#### Scenario: Multiple functions create multiple derived features
- GIVEN a math feature creator configured with more than one supported aggregate function
- WHEN the transformer is applied to a compatible dataset
- THEN the transformed dataset SHALL include one derived feature per configured function using deterministic output names

### Requirement: RelativeFeatures
The library MUST provide a relative feature creator that derives new numerical features by applying supported arithmetic operations between selected variables and one or more configured reference variables.

#### Scenario: Relative features are generated against reference variables
- GIVEN a dataset with selected numerical variables, reference numerical variables, and untouched non-selected columns
- WHEN a relative feature creator is applied with one or more supported arithmetic functions
- THEN the transformed dataset SHALL include derived feature columns for each valid variable-reference-function combination
- AND the original selected and reference columns SHALL remain unchanged unless configured to be dropped

#### Scenario: Invalid arithmetic cases are handled explicitly
- GIVEN a relative feature creator configured for operations that can fail because of invalid numeric inputs such as zero denominators
- WHEN the transformer is applied to data that triggers those cases
- THEN the result SHALL follow the transformer's configured validation or replacement contract instead of silently producing ambiguous output

### Requirement: CyclicalFeatures
The library MUST provide a cyclical feature creator that maps selected numerical variables into sine and cosine components using per-variable maximum values learned during fit or supplied explicitly by configuration.

#### Scenario: Fit learns max values when not provided
- GIVEN a dataset with selected numerical variables and no explicit maximum-value mapping
- WHEN a cyclical feature creator is fit on the dataset
- THEN the fitted instance SHALL learn and expose a maximum value for each selected variable through public attribute names that end with an underscore

#### Scenario: Transform adds sine and cosine features
- GIVEN a fitted cyclical feature creator or a configuration with explicit maximum values
- WHEN the transformer is applied to a compatible dataset
- THEN the transformed dataset SHALL include `<variable>_sin` and `<variable>_cos` features for each selected variable
- AND the original selected variables SHALL remain unchanged unless configured to be dropped

### Requirement: Creation Validation and Lifecycle
Phase 4 creation transformers MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes when fitting derives state, and MUST affect only the selected variables and newly created outputs.

#### Scenario: Invalid configuration or schema is rejected
- GIVEN a creation transformer configured with missing columns, duplicated variables, incompatible column types, invalid function names, duplicate output names, or insufficient variables for the requested operation
- WHEN the user attempts to fit or transform data with that transformer
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN a Phase 4 creation transformer is applied to the selected columns
- THEN only the selected columns, configured drop behavior, and newly created output columns SHALL differ in the transformed result

### Requirement: Native Spark Creation Execution
Phase 4 creation transformers MUST execute through native Spark SQL, DataFrame, or Spark ML operations and MUST NOT require Python UDF execution for fitting or transforming derived values.

#### Scenario: Creation plans remain Spark-native
- GIVEN a Phase 4 creation transformer fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes
