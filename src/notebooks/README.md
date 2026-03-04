# Notebooks

Organized by Medallion layer. Each notebook follows the naming convention:
`nb_seedz_{layer}_{action}_{entity}`

## Directories

- `bronze/` — Landing → Bronze ingestion (7 notebooks)
- `silver/` — Bronze → Silver transformation (7 notebooks)
- `gold/` — Silver → Gold API formatting (11 notebooks)
- `utils/` — Shared utilities (delta, logging, API auth)

## Execution Order

```
utils/nb_common_utils_delta.py         ← Run first (shared functions)
bronze/nb_seedz_bronze_ingest_*.py     ← Phase 1 (7 notebooks, parallelizable)
bronze/nb_seedz_bronze_load_config.py  ← Phase 1 (config tables)
silver/nb_seedz_silver_transform_*.py  ← Phase 2 (7 notebooks, dependency order)
gold/nb_seedz_gold_build_*.py          ← Phase 3 (11 notebooks, dependency order)
```

## How Claude Code Should Generate Notebooks

1. Read CLAUDE.md for project context
2. Read agent_docs/task_queue.md for the specific task
3. Read agent_docs/schemas_reference.md for input/output schemas
4. Read agent_docs/business_rules.md for transformation logic
5. Generate .py file following the notebook template in CLAUDE.md
6. Generate matching test in src/tests/
