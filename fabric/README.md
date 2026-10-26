# Microsoft Fabric notebooks

The PySpark + Delta Lake implementation of the same medallion design lives in `src/`:

| Script | Stage |
|--------|--------|
| `src/01_bronze_to_silver.py` | Bronze JSONL → `silver_events` + dead-letter (Delta MERGE) |
| `src/02_silver_to_gold.py` | Silver → SCD2 `dim_user`, `fact_edit`, hourly agg, alerts |

## How to run in Fabric

1. Create a Lakehouse and upload local `data/bronze` to `Files/bronze`.
2. Create two notebooks, attach the Lakehouse, and paste (or `%run`) the corresponding `src/*.py` cells.
3. Run **01** then **02**. Build a Power BI report on the Delta tables (Direct Lake).

The logic mirrors the local `pulse/` package: data contract, file-level incremental load, SCD Type 2, point-in-time joins, and spike / edit-war detection.
