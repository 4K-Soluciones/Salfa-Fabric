# Coding Standards — SALFA Fabric Project

> Extracted from Estandares-de-Codigo.md (4K Soluciones)

## Python / PySpark Style

- **PEP 8** strict compliance
- 4 spaces indentation, 79 chars line length, 72 chars for comments
- Functions: `snake_case`
- Classes: `CamelCase`
- Constants: `UPPER_SNAKE_CASE`
- Imports on separate lines, at top of file

## Functional Programming Principles

- **Immutability:** Don't modify DataFrames in place. Chain transformations.
- **Pure Functions:** No side effects. Same input → same output.
- **Higher-order functions:** Use `map`, `filter`, `reduce` patterns via PySpark API.

```python
# CORRECT — functional chain
df_result = (
    df_source
    .withColumn("name", F.trim(F.concat_ws(" ", F.col("name1"), F.col("name2"))))
    .withColumn("tax_id", F.regexp_replace(F.col("stcd1"), "[^0-9]", ""))
    .withColumn("phone", F.regexp_replace(F.col("tel_number"), "[^0-9]", ""))
    .filter(F.col("loevm") != "X")
)

# WRONG — mutating same variable
df = spark.read.table("source")
df = df.withColumn("name", ...)  # mutating!
df = df.filter(...)              # mutating!
```

## SOLID Principles

- **Single Responsibility:** One notebook = one entity transformation.
- **Open/Closed:** Config tables for business rules (add new brands without code changes).
- **Dependency Inversion:** Utility functions in shared notebook, referenced via `%run`.

## Code Review Requirements

- 2 peers + 1 architect review before merge
- Unit tests required for every new function
- Golden fixture validation for Silver/Gold notebooks

## Environment Management

- Miniconda for Python environments
- Poetry for dependency management
- In Fabric: use built-in PySpark environment (no custom installs needed)

## Notebook-Specific Standards

1. Every notebook starts with metadata comment block (name, layer, source, target)
2. Configuration cell with UPPERCASE constants
3. Imports cell (pyspark.sql.functions as F, pyspark.sql.types)
4. Read cell with row count print
5. Transformation cells (one logical step per cell)
6. Write cell with row count print
7. Validation cell with assertions
8. No hardcoded values — use config tables or notebook parameters
