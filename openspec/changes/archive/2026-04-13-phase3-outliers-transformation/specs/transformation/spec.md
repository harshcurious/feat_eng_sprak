# Transformation Specification

## Requirements

### Requirement: LogTransformer
The library MUST provide a log transformer that applies a logarithmic transform to each selected numerical variable using native Spark expressions.

#### Scenario: Positive inputs are transformed logarithmically
- GIVEN a dataset with selected numerical variables containing strictly positive values and untouched non-selected columns
- WHEN a log transformer is applied to the dataset
- THEN each selected variable SHALL be replaced with its logarithmic transform
- AND non-selected columns SHALL remain unchanged

#### Scenario: Zero or negative inputs are rejected
- GIVEN a dataset with selected numerical variables containing zero or negative values
- WHEN a log transformer is applied to the dataset
- THEN the operation SHALL fail with a validation error instead of producing invalid output

#### Scenario: Logarithmic output is Spark-native
- GIVEN a log transformer processing a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes

### Requirement: PowerTransformer
The library MUST provide a power transformer that raises each selected numerical variable to a configured exponent using native Spark expressions.

#### Scenario: Default exponent is applied
- GIVEN a dataset with selected numerical variables and untouched non-selected columns
- WHEN a power transformer created with the default exponent is applied to the dataset
- THEN each selected variable SHALL be raised to the configured default exponent
- AND non-selected columns SHALL remain unchanged

#### Scenario: Custom exponent is applied
- GIVEN a dataset with selected numerical variables and a power transformer configured with an explicit exponent
- WHEN the transformer is applied to the dataset
- THEN each selected variable SHALL be raised to that configured exponent

#### Scenario: Power transform is Spark-native
- GIVEN a power transformer processing a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes

### Requirement: Transformation Validation and Lifecycle
Phase 3 transformation transformers MUST validate configuration and schema compatibility before producing output and MUST affect only the selected variables.

#### Scenario: Invalid target columns or unsupported configuration are rejected
- GIVEN a transformation transformer configured with missing columns, duplicated target columns, incompatible column types, or unsupported parameter values
- WHEN the user attempts to apply the transformer to data
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN a Phase 3 transformation transformer is applied to the selected columns
- THEN only the selected columns SHALL change in the transformed result
