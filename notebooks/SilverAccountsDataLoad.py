# Databricks notebook source
import pyspark.sql.functions as f

df=spark.readStream.table("databricksaml_cat.bronze.accounts")

df_clean=df.select(*[f.coalesce(f.col(x),f.get_json_object(f.col("_rescued_data"),f"$.{x}")).alias(x).cast("BIGINT") for x in df.columns  if x != "_rescued_data"])



# COMMAND ----------

df_clean.writeStream.format("delta").outputMode("append")\
    .option("checkpointLocation", "abfss://silver@databricksamlstoracc.dfs.core.windows.net/accounts/checkpoint/")\
    .trigger(availableNow=True)\
    .toTable("databricksaml_cat.silver.accounts")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from databricksaml_cat.silver.accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history databricksaml_cat.silver.accounts