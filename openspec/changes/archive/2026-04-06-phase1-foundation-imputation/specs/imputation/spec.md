# Imputation Specification

## Requirements

### Requirement: Mean and Median Numerical Imputation
The library MUST provide a numerical imputer that can learn per-column replacement statistics using either mean or median strategy and apply those learned values to missing data in the selected numerical columns.

#### Scenario: Mean strategy fills missing numerical values
- GIVEN a dataset with null values in selected numerical columns
- AND a mean or median imputer configured with the mean strategy
- WHEN the imputer is fit and then used to transform the dataset
- THEN each selected numerical column SHALL replace null values with that column's learned mean

#### Scenario: Median strategy fills missing numerical values
- GIVEN a dataset with null values in selected numerical columns
- AND a mean or median imputer configured with the median strategy
- WHEN the imputer is fit and then used to transform the dataset
- THEN each selected numerical column SHALL replace null values with that column's learned median statistic

### Requirement: Arbitrary Numerical Imputation
The library MUST provide a numerical imputer that replaces missing values in selected numerical columns with user-specified replacement value or values.

#### Scenario: A single arbitrary value is applied
- GIVEN a dataset with null values in selected numerical columns
- AND an arbitrary number imputer configured with a replacement value
- WHEN the transformer is applied
- THEN null values in the selected numerical columns SHALL be replaced with the configured value

### Requirement: Categorical Imputation
The library MUST provide a categorical imputer that replaces missing values in selected categorical columns with a configured fill value and MUST default that fill value to `"missing"` when the user does not provide one.

#### Scenario: Default categorical fill value is used
- GIVEN a dataset with null values in selected categorical columns
- AND a categorical imputer created without an explicit fill value
- WHEN the transformer is applied
- THEN null values in the selected categorical columns SHALL be replaced with `"missing"`

#### Scenario: Custom categorical fill value is used
- GIVEN a dataset with null values in selected categorical columns
- AND a categorical imputer configured with a custom fill value
- WHEN the transformer is applied
- THEN null values in the selected categorical columns SHALL be replaced with the configured value

### Requirement: Drop Missing Data
The library MUST provide a transformer that removes rows containing missing values in the selected columns.

#### Scenario: Rows with selected-column nulls are removed
- GIVEN a dataset containing rows with null values in selected columns
- WHEN a drop-missing-data transformer is applied for those columns
- THEN rows with null values in any selected column SHALL be excluded from the result

### Requirement: Column-Scoped Behavior
Phase 1 imputation transformers MUST affect only the columns selected for that transformer and MUST leave non-selected columns unchanged.

#### Scenario: Non-selected columns remain unchanged
- GIVEN a dataset with selected and non-selected columns
- WHEN an imputation transformer is applied to the selected columns
- THEN only the selected columns SHALL be changed by the transform operation

### Requirement: Parameter and Schema Validation
Phase 1 imputation transformers MUST reject unsupported parameter combinations and incompatible target columns before producing transformed output.

#### Scenario: Invalid target columns are rejected
- GIVEN a transformer configured for columns that do not satisfy that transformer's expected input type or configuration rules
- WHEN the user attempts to fit or transform data with that transformer
- THEN the operation SHALL fail with a validation error instead of silently producing output

### Requirement: Native Spark Execution
Phase 1 imputation transformers MUST operate through native PySpark SQL or DataFrame expressions and MUST NOT require Python UDF execution for their core missing-data behavior.

#### Scenario: Missing-data handling stays within native Spark operations
- GIVEN a Phase 1 imputation transformer processing a Spark DataFrame
- WHEN the transformer performs its missing-data behavior
- THEN the behavior SHALL be expressible through native Spark column, SQL, or DataFrame operations without Python UDFs
