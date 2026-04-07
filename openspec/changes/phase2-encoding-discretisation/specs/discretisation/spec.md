# Discretisation Specification

## Requirements

### Requirement: Equal-Width Discretisation
The library MUST provide a fitted equal-width discretiser that learns contiguous bin boundaries for each selected numerical variable from the fit dataset range and replaces each selected value with its learned bin assignment.

#### Scenario: Equal-width bins are learned from the observed range
- GIVEN a dataset with selected numerical variables and a configured number of bins
- WHEN an equal-width discretiser is fit
- THEN the fitted model SHALL learn contiguous boundaries for each selected variable that partition the observed value range into the configured number of equal-width intervals

#### Scenario: Learned bins are applied during transform
- GIVEN a fitted equal-width discretiser model and a dataset containing the selected numerical variables
- WHEN the model transforms the dataset
- THEN each selected variable SHALL be replaced with ordered bin assignments derived from the learned boundaries
- AND non-selected columns SHALL remain unchanged

#### Scenario: Future extreme values remain assignable
- GIVEN a fitted equal-width discretiser model and transform-time rows whose selected values fall outside the fit-time minimum or maximum
- WHEN the model transforms those rows
- THEN the learned outer boundaries SHALL still assign those values to the first or last bin instead of failing solely because the values exceed the fit-time range

### Requirement: Equal-Frequency Discretisation
The library MUST provide a fitted equal-frequency discretiser that learns contiguous quantile-based boundaries for each selected numerical variable and replaces each selected value with its learned bin assignment.

#### Scenario: Quantile-based bins contain approximately equal row counts
- GIVEN a dataset with selected numerical variables and a configured number of bins
- WHEN an equal-frequency discretiser is fit
- THEN the fitted model SHALL learn ordered boundaries that divide the fit dataset into approximately equal-frequency intervals for each selected variable

#### Scenario: Approximate Spark quantiles are an acceptable learning mechanism
- GIVEN a dataset large enough that Spark quantile estimation is approximate
- WHEN an equal-frequency discretiser is fit
- THEN the learned boundaries MAY reflect Spark-native approximate quantile behavior
- AND the transformed output SHALL still preserve the configured ordering of contiguous bins

#### Scenario: Future extreme values remain assignable
- GIVEN a fitted equal-frequency discretiser model and transform-time rows whose selected values fall outside the fit-time minimum or maximum
- WHEN the model transforms those rows
- THEN the learned outer boundaries SHALL still assign those values to the first or last bin instead of failing solely because the values exceed the fit-time range

### Requirement: Arbitrary Discretisation
The library MUST provide an arbitrary discretiser that uses user-supplied bin boundaries for selected numerical variables without learning data-dependent boundaries during fit.

#### Scenario: User-supplied boundaries drive bin assignment
- GIVEN a discretiser configured with explicit bin boundaries per selected variable
- WHEN the discretiser is applied to a dataset containing those variables
- THEN each selected value SHALL be assigned according to the configured ordered boundaries for its variable

#### Scenario: Boundary inclusivity is explicit and stable
- GIVEN user-supplied boundaries that place values exactly on interval limits
- WHEN the arbitrary discretiser transforms those values
- THEN interval membership SHALL be evaluated as left-open and right-closed for interior bins
- AND the first bin SHALL include its lower boundary

#### Scenario: Out-of-range handling follows the configured policy
- GIVEN user-supplied boundaries and transform-time values that fall below the minimum boundary or above the maximum boundary
- WHEN the arbitrary discretiser is configured to ignore or raise for out-of-range values and transforms those rows
- THEN the transformed result SHALL respectively preserve a null-like missing output or fail with a validation error

### Requirement: Discretised Output Representation
Phase 2 discretisers MUST support returning either ordered bin identifiers or interval-boundary labels for selected variables according to configuration.

#### Scenario: Numeric bin identifiers are returned by default
- GIVEN a fitted Phase 2 discretiser configured with default output representation
- WHEN it transforms selected numerical variables
- THEN the transformed selected variables SHALL contain ordered bin identifiers suitable for downstream Spark-native numerical processing

#### Scenario: Boundary labels can be returned for interpretability
- GIVEN a fitted Phase 2 discretiser configured to return interval boundaries
- WHEN it transforms selected numerical variables
- THEN the transformed selected variables SHALL contain deterministic interval-boundary labels representing the assigned bins

### Requirement: Discretisation Validation and Lifecycle
Phase 2 discretisers MUST validate configuration and schema compatibility before producing output, MUST expose learned state through trailing-underscore public attributes when fitting derives boundaries, and MUST affect only the selected variables.

#### Scenario: Invalid target columns or unsupported configuration are rejected
- GIVEN a discretiser configured with missing columns, duplicated target columns, incompatible column types, unsorted boundaries, or an invalid bin-count configuration
- WHEN the user attempts to fit or transform data with that discretiser
- THEN the operation SHALL fail with a validation error instead of silently producing output

#### Scenario: Learned discretisers expose fitted public attributes
- GIVEN an equal-width or equal-frequency discretiser whose fit step derives boundary definitions
- WHEN fitting completes successfully
- THEN the fitted object SHALL expose those learned boundary definitions through public attribute names that end with an underscore

#### Scenario: Stateless arbitrary discretisation does not invent learned state
- GIVEN an arbitrary discretiser whose behavior is fully determined by user configuration
- WHEN the transformer is fit or applied
- THEN it SHALL preserve the configured boundaries without requiring additional learned state from the dataset

### Requirement: Native Spark Discretisation Execution
Phase 2 discretisers MUST execute through native Spark SQL, DataFrame, or Spark ML operations and MUST NOT require Python UDF execution for fitting or transforming numerical values.

#### Scenario: Discretisation plans remain Spark-native
- GIVEN a Phase 2 discretiser fitting or transforming a Spark DataFrame
- WHEN the operation is planned and executed by Spark
- THEN the resulting logical or physical plan SHALL be expressible without Python UDF execution nodes
