# %% [markdown]
# # 02 · Silver → Gold (Microsoft Fabric · PySpark · Delta Lake)
#
# Builds from `silver_events`:
# - `dim_user` – SCD Type 2 editor history (tier: newcomer → active → power)
# - `fact_edit` – one row per edit with a point-in-time join to dim_user
# - `agg_page_hourly` – hourly activity per page
# - `alerts` – edit spikes (rolling z-score) and edit wars

# %%
from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable

OPEN_END = F.lit("9999-12-31 23:59:59").cast("timestamp")
TIERS = [(100, "power"), (10, "active"), (0, "newcomer")]
SPIKE_MIN_EDITS, SPIKE_Z, BASELINE_H = 8, 3.0, 24
WAR_MIN_EDITS, WAR_MIN_USERS, WAR_MIN_REVERTS = 4, 2, 2

edits = (spark.table("silver_events")
         .filter(F.col("event_type").isin("edit", "new"))
         .withColumn("hour_ts", F.date_trunc("hour", "event_ts")))

# %% dim_user SCD2
w_user = W.partitionBy("user_name").orderBy("event_ts", "event_id")
tier_col = F.when(F.col("edit_no") >= TIERS[0][0], TIERS[0][1])
for threshold, name in TIERS[1:]:
    tier_col = tier_col.when(F.col("edit_no") >= threshold, name)

per_edit = (edits.select("user_name", "event_ts", "event_id", "is_bot")
    .withColumn("edit_no", F.row_number().over(w_user))
    .withColumn("tier", tier_col)
    .withColumn("prev_tier", F.lag("tier").over(w_user))
    .withColumn("prev_bot", F.lag("is_bot").over(w_user)))

versions = (per_edit
    .filter(F.col("prev_tier").isNull() | (F.col("tier") != F.col("prev_tier"))
            | (F.col("is_bot") != F.col("prev_bot")))
    .select("user_name", "is_bot", "tier", F.col("edit_no").alias("edits_at_change"),
            F.col("event_ts").alias("valid_from"))
    .withColumn("valid_to", F.coalesce(
        F.lead("valid_from").over(W.partitionBy("user_name").orderBy("valid_from")), OPEN_END))
    .withColumn("is_current", F.col("valid_to") == OPEN_END)
    .withColumn("user_sk", F.xxhash64("user_name", "valid_from")))

if not spark.catalog.tableExists("dim_user"):
    versions.limit(0).write.format("delta").saveAsTable("dim_user")

(DeltaTable.forName(spark, "dim_user").alias("t")
    .merge(versions.alias("s"), "t.user_sk = s.user_sk")
    .whenMatchedUpdate(
        condition="t.valid_to <> s.valid_to OR t.is_current <> s.is_current",
        set={"valid_to": "s.valid_to", "is_current": "s.is_current"})
    .whenNotMatchedInsertAll()
    .execute())

# %% fact_edit point-in-time
du = spark.table("dim_user").select("user_sk", F.col("user_name").alias("du_user"),
                                    "tier", "valid_from", "valid_to")
fact = (edits.join(du, (edits.user_name == du.du_user)
                       & (edits.event_ts >= du.valid_from) & (edits.event_ts < du.valid_to), "left")
    .select("event_id", "event_ts", "hour_ts",
            F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_key"),
            "title", "namespace", "user_sk", F.col("tier").alias("tier_at_edit"),
            (F.col("event_type") == "new").alias("is_new_page"),
            "is_minor", "is_bot", "is_revert", "bytes_delta", "lateness_s"))

fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_edit")

# %% agg_page_hourly
agg = (spark.table("fact_edit").groupBy("hour_ts", "title").agg(
        F.count("*").alias("edits"),
        F.sum((~F.col("is_bot")).cast("int")).alias("human_edits"),
        F.sum(F.col("is_bot").cast("int")).alias("bot_edits"),
        F.countDistinct(F.when(~F.col("is_bot"), F.col("user_sk"))).alias("human_editors"),
        F.sum(F.col("is_revert").cast("int")).alias("reverts"),
        F.sum(F.when(F.col("bytes_delta") > 0, F.col("bytes_delta")).otherwise(0)).alias("bytes_added"),
        F.sum(F.when(F.col("bytes_delta") < 0, -F.col("bytes_delta")).otherwise(0)).alias("bytes_removed")))
agg.write.format("delta").mode("overwrite").saveAsTable("agg_page_hourly")

# %% alerts
bounds = agg.agg(F.min("hour_ts").alias("lo"), F.max("hour_ts").alias("hi")).first()
hours = spark.sql(f"""SELECT explode(sequence(timestamp'{bounds.lo}', timestamp'{bounds.hi}',
                                               interval 1 hour)) AS hour_ts""")
grid = (hours.crossJoin(agg.select("title").distinct())
        .join(agg.select("hour_ts", "title", "edits"), ["hour_ts", "title"], "left")
        .fillna(0, subset=["edits"]))

w_base = W.partitionBy("title").orderBy("hour_ts").rowsBetween(-BASELINE_H, -1)
spikes = (grid
    .withColumn("base_mean", F.avg("edits").over(w_base))
    .withColumn("base_std", F.stddev_pop("edits").over(w_base))
    .withColumn("history_h", F.count("*").over(w_base))
    .withColumn("z", (F.col("edits") - F.col("base_mean")) / F.greatest(F.col("base_std"), F.lit(1.0)))
    .filter((F.col("edits") >= SPIKE_MIN_EDITS) & (F.col("z") >= SPIKE_Z) & (F.col("history_h") >= 6))
    .select(F.lit("spike").alias("alert_type"), "hour_ts", "title",
            F.round("z", 2).alias("metric_value"),
            F.format_string("%d edits vs baseline %.2f±%.2f/h", "edits", "base_mean", "base_std")
             .alias("details")))

wars = (agg.filter((F.col("human_edits") >= WAR_MIN_EDITS) & (F.col("human_editors") >= WAR_MIN_USERS)
                   & (F.col("reverts") >= WAR_MIN_REVERTS))
    .select(F.lit("edit_war").alias("alert_type"), "hour_ts", "title",
            F.col("reverts").cast("double").alias("metric_value"),
            F.format_string("%d human edits by %d editors, %d reverts",
                            "human_edits", "human_editors", "reverts").alias("details")))

alerts = spikes.unionByName(wars).withColumn("detected_at", F.current_timestamp())
alerts.write.format("delta").mode("overwrite").saveAsTable("alerts")
print("silver → gold done")
