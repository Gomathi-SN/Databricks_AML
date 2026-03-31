# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_sanctionedclients
# MAGIC as
# MAGIC select * from databricksaml_cat.silver.clients where sanctions_flag= '1' 

# COMMAND ----------

from pyspark.sql.functions import *

df_sanc = spark.sql("select * from vw_sanctionedclients")

df_clients= spark.sql("select * from databricksaml_cat.silver.clients")

df_overlap=df_sanc.alias("vw").crossJoin(df_clients.alias("cl")).withColumn("name_distance",levenshtein(lower(trim(df_sanc.client_name)), lower(trim(df_clients.client_name))))\
    .filter((col("vw.client_id") != col("cl.client_id")) & (col("name_distance") < 3))\
    .select("name_distance","cl.sector_risk", "cl.client_id", col("cl.client_name").alias("ClientName"), col("vw.client_id").alias("SanctionedClientID"), col("vw.client_name").alias("sanctionedName"))

df_overlap.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("databricksaml_cat.gold.CustSanctionAlert")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksaml_cat.gold.CustSanctionAlert limit 5 