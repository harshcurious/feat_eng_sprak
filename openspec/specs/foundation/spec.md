# Foundation Specification

## Requirements

### Requirement: Package Quality Gates
The project MUST provide a Python package baseline with repository-visible configuration for formatting, linting, static typing, and test execution so Phase 1 components can be developed and verified consistently.

#### Scenario: Local quality tools are discoverable
- GIVEN a developer clones the repository
- WHEN the developer inspects the project configuration
- THEN the repository SHALL define how to run formatting, linting, type checking, and tests for the package

### Requirement: Spark Test Harness
The project MUST provide a reusable local Spark test harness for pytest so transformer behavior can be exercised against a real SparkSession.

#### Scenario: Tests request a Spark session
- GIVEN a pytest test that depends on Spark execution
- WHEN the test requests the shared Spark fixture
- THEN the fixture SHALL provide a local SparkSession suitable for DataFrame-based transformer tests

### Requirement: Base Transformer Contract
The library MUST expose a shared PySpark ML transformer base that follows the Transformer and Params contract and standardizes public parameter handling for library transformers.

#### Scenario: A library transformer inherits the base class
- GIVEN a transformer built on the shared base abstraction
- WHEN the transformer exposes configurable inputs
- THEN those inputs SHALL be represented through PySpark Param objects and participate in the standard fit and transform lifecycle

### Requirement: Learned State Naming
Any transformer that learns state during fitting MUST expose its learned public attributes with a trailing underscore naming convention.

#### Scenario: A fitted transformer stores derived values
- GIVEN a transformer that computes values during fit
- WHEN fitting completes successfully
- THEN the learned values SHALL be available on the fitted instance using attribute names that end with an underscore

### Requirement: Public API Typing
The library SHOULD provide type hints on public Phase 1 APIs so users and static analysis tools can reason about supported inputs and outputs.

#### Scenario: A user inspects a public Phase 1 API
- GIVEN a public class or method introduced in Phase 1
- WHEN the user or a type checker inspects that API
- THEN parameter and return types SHOULD be declared for the public surface
