# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select date_format(TransactionDate,'yyyyMMdd') ,  Sender_account, Receiver_account,
# MAGIC sum(Amount),  count(*)
# MAGIC  from databricksaml_cat.silver.transactions 
# MAGIC group by date_format(TransactionDate,'yyyyMMdd') , Sender_account, Receiver_account
# MAGIC having( sum(Amount) > 10000 AND count(*) > 2 )
# MAGIC limit 5

# COMMAND ----------

from pyspark.sql.functions import *

silver_df = spark.readStream.table("databricksaml_cat.silver.transactions")

gold_df = silver_df.groupBy(date_format(col("TransactionDate"), "yyyyMMdd").alias("TransactionDate"), "Sender_account", "Receiver_account").agg( sum("Amount").alias("TotalAmount"),count("*").alias("NumTransactions"))\
    .withColumn("is_HighVolTrans", when((col("TotalAmount") > 100000) & (col("NumTransactions") > 5),0).otherwise(1))

gold_df.writeStream.format("delta").outputMode("complete").option("checkpointLocation", "abfss://gold@databricksamlstoracc.dfs.core.windows.net/checkpoint").trigger(availableNow=True).table("databricksaml_cat.gold.HighVolTrans")
