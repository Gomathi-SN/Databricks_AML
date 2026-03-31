# Databricks notebook source
import pyspark.sql.functions as f

df=spark.readStream.table("databricksaml_cat.bronze.clients")

df_clean=df.select(*[f.coalesce(f.col(x),f.get_json_object(f.col("_rescued_data"),f"$.{x}")).alias(x) for x in df.columns if x != "_rescued_data"])



# COMMAND ----------

df_clean.writeStream.format("delta").outputMode("append")\
    .option("checkpointLocation", "abfss://silver@databricksamlstoracc.dfs.core.windows.net/clients/checkpoint")\
    .trigger(availableNow=True)\
    .toTable("databricksaml_cat.silver.clients")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from databricksaml_cat.silver.clients