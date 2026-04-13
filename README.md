# spark-feature-engine

`spark-feature-engine` is a PySpark-native feature engineering library built in the style of `feature_engine`, but adapted to the `pyspark.ml` `Estimator` / `Model` / `Transformer` pattern.

It avoids Python UDFs and uses native Spark SQL and DataFrame expressions instead.

## Status

Implemented so far:

- Imputation
  - `MeanMedianImputer`
  - `ArbitraryNumberImputer`
  - `CategoricalImputer`
  - `DropMissingData`
- Encoding
  - `OneHotEncoder`
  - `OrdinalEncoder`
  - `CountFrequencyEncoder`
  - `RareLabelEncoder`
- Discretisation
  - `EqualWidthDiscretiser`
  - `EqualFrequencyDiscretiser`
  - `ArbitraryDiscretiser`
- Outliers
  - `Winsorizer`
  - `OutlierTrimmer`
- Transformation
  - `LogTransformer`
  - `PowerTransformer`
- Creation
  - `MathFeatures`
  - `RelativeFeatures`
  - `CyclicalFeatures`

## Design goals

- follow the Spark ML API
- use `Param`-based configuration
- keep learned state on fitted models with trailing-underscore attributes
- prefer native Spark operations over Python UDFs
- develop features test-first

## Installation

For local development:

```bash
uv sync --extra dev
```

## Quick example

```python
from spark_feature_engine import MeanMedianImputer, PowerTransformer

imputer = MeanMedianImputer(variables=["income"], strategy="mean")
imputer_model = imputer.fit(df)
df = imputer_model.transform(df)

transformer = PowerTransformer(variables=["income"], exponent=2)
df = transformer.transform(df)
```

Example with a learned creation transformer:

```python
from spark_feature_engine import CyclicalFeatures

cyclical = CyclicalFeatures(variables=["month", "hour"])
cyclical_model = cyclical.fit(df)
df = cyclical_model.transform(df)
```

## Development

Run the main local checks with:

```bash
uv run pytest
uv run mypy src tests
uv run python -m build
```

## Project structure

- `src/spark_feature_engine/` — package code
- `tests/` — pytest suite with a local `SparkSession`
- `openspec/` — planning and change history

## License

BSD 3-Clause. See `LICENSE`.
