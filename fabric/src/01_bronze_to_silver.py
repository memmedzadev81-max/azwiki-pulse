# %% [markdown]
# # 01 · Bronze → Silver (Microsoft Fabric · PySpark · Delta Lake)
#
# **Input:** JSON Lines files written by `pulse_cli.py stream` (or `demo`), uploaded to the
# Lakehouse under `Files/bronze/`.
#
# **Output (Delta tables):**
# - `silver_events` – validated, typed, de-duplicated events (MERGE on `event_id`)
# - `silver_dead_letter` – records that broke the data contract, with a reason
# - `etl_file_manifest` – which bronze files were processed (file-level incremental load)
#
# Attach this notebook to a Lakehouse before running it.

# %%
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

BRONZE_PATH = "Files/bronze"
TARGET_WIKI = "azwiki"
REVERT_REGEX = r"(?i)(revert|undo|\brv\b|geri qaytar|ləğv|legv)"

# %%
event_schema = T.StructType([
    T.StructField("meta", T.StructType([T.StructField("id", T.StringType())])),
    T.StructField("type", T.StringType()),
    T.StructField("namespace", T.LongType()),
    T.StructField("title", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("user", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("minor", T.BooleanType()),
    T.StructField("length", T.StructType([T.StructField("old", T.LongType()),
                                          T.StructField("new", T.LongType())])),
    T.StructField("wiki", T.StringType()),
    T.StructField("log_type", T.StringType()),
    T.StructField("log_params", T.StringType()),
])
envelope_schema = T.StructType([
    T.StructField("ingested_at", T.StringType()),
    T.StructField("sse_id", T.StringType()),
    T.StructField("event", event_schema),
])

raw = (spark.read.schema(envelope_schema)
       .option("recursiveFileLookup", "true")
       .option("pathGlobFilter", "*.jsonl")
       .json(BRONZE_PATH)
       .withColumn("source_file", F.col("_metadata.file_path")))

if spark.catalog.tableExists("etl_file_manifest"):
    done = spark.table("etl_file_manifest").select("source_file")
    raw = raw.join(done, "source_file", "left_anti")

raw = raw.cache()
print(f"new bronze files: {raw.select('source_file').distinct().count()}, lines: {raw.count()}")

# %%
e = F.col("event")
parsed = (raw.select(
        F.col("source_file"),
        e.meta.id.alias("event_id"),
        e.type.alias("event_type"),
        e.namespace.alias("namespace"),
        F.trim(e.title).alias("title"),
        e.user.alias("user_name"),
        F.coalesce(e.bot, F.lit(False)).alias("is_bot"),
        F.coalesce(e.minor, F.lit(False)).alias("is_minor"),
        F.coalesce(e.comment, F.lit("")).alias("comment"),
        e.wiki.alias("wiki"),
        e.log_type.alias("log_type"),
        F.get_json_object(e.log_params, "$.target").alias("move_target"),
        e.length.old.alias("length_old"),
        e.length.new.alias("length_new"),
        F.expr("try_cast(event.timestamp AS BIGINT)").alias("ts_epoch"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.to_json(F.struct("ingested_at", "sse_id", "event")).alias("raw"),
    )
    .withColumn("event_ts", F.timestamp_seconds("ts_epoch")))

reason = (F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id")
           .when(~F.coalesce(F.col("wiki") == TARGET_WIKI, F.lit(False)), "wrong_wiki")
           .when(~F.coalesce(F.col("event_type").isin("edit", "new", "log", "categorize"),
                             F.lit(False)), "unknown_type")
           .when(F.col("title").isNull() | (F.col("title") == ""), "missing_title")
           .when(F.col("user_name").isNull() | (F.col("user_name") == ""), "missing_user")
           .when(F.col("namespace").isNull(), "bad_namespace")
           .when(F.col("ts_epoch").isNull() | (F.col("ts_epoch") < 979516800), "bad_timestamp")
           .when(F.col("ingested_at").isNull(), "bad_ingested_at")
           .when((F.col("event_type") == "log") & (F.col("log_type") == "move")
                 & F.col("move_target").isNull(), "move_without_target"))
checked = parsed.withColumn("dq_reason", reason)

# %%
dead = (checked.filter(F.col("dq_reason").isNotNull())
        .select("source_file", F.col("dq_reason").alias("reason"), "raw",
                F.current_timestamp().alias("quarantined_at")))
dead.write.format("delta").mode("append").saveAsTable("silver_dead_letter")
print(f"dead-lettered: {dead.count()}")

# %%
silver_new = (checked.filter(F.col("dq_reason").isNull())
    .withColumn("bytes_delta",
                F.when(F.col("event_type") == "new", F.col("length_new"))
                 .otherwise(F.col("length_new") - F.col("length_old")))
    .withColumn("lateness_s",
                F.greatest(F.lit(0), F.col("ingested_at").cast("long") - F.col("ts_epoch")))
    .withColumn("is_revert", F.col("comment").rlike(REVERT_REGEX))
    .select("event_id", "event_ts", "ingested_at", "lateness_s", "event_type", "namespace",
            "title", "user_name", "is_bot", "is_minor", "length_old", "length_new",
            "bytes_delta", "comment", "is_revert", "log_type", "move_target", "source_file")
    .dropDuplicates(["event_id"]))

if not spark.catalog.tableExists("silver_events"):
    silver_new.limit(0).write.format("delta").saveAsTable("silver_events")

(DeltaTable.forName(spark, "silver_events").alias("t")
    .merge(silver_new.alias("s"), "t.event_id = s.event_id")
    .whenNotMatchedInsertAll()
    .execute())

# %%
(raw.groupBy("source_file").agg(F.count("*").alias("lines_read"))
    .withColumn("processed_at", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable("etl_file_manifest"))
raw.unpersist()
print("bronze → silver done")
