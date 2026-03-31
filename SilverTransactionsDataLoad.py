# Databricks notebook source
import pyspark.sql.functions as f

df=spark.readStream.table("databricksaml_cat.bronze.transactions")
df1 = df.withColumn("Date1",f.date_format(f.to_date(f.col("Date")),"d/MM/yyyy"))

df_clean=df.withColumn("TransactionDate",f.to_timestamp(f.concat(f.col("Date"),f.date_format(f.col("Time")," HH:mm:ss")),"d/MM/yyyy HH:mm:ss"))\
    .withColumn("Amount",f.coalesce(f.col("Amount"),f.get_json_object(f.col("_rescued_data"),f"$.Amount").cast("DOUBLE"))) \
    .withColumn("Sender_account",f.coalesce(f.col("Sender_account").cast("BIGINT"),f.get_json_object(f.col("_rescued_data"),f"$.Sender_account").cast("BIGINT"))) \
    .withColumn("Receiver_account",f.coalesce(f.col("Receiver_account").cast("BIGINT"),f.get_json_object(f.col("_rescued_data"),f"$.Receiver_account").cast("BIGINT"))) 
    
df_write= df_clean.select("TransactionDate","Amount","Sender_account","Receiver_account","Is_laundering","Laundering_type","Payment_currency","Payment_type","Received_currency","Receiver_bank_location","Sender_bank_location")



# COMMAND ----------

df_write.writeStream.format("delta").outputMode("append")\
     .option("checkpointLocation", "abfss://silver@databricksamlstoracc.dfs.core.windows.net/transactions/checkpoint/")\
    .trigger(availableNow=True)\
    .toTable("databricksaml_cat.silver.transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe databricksaml_cat.silver.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  -- select * from databricksaml_cat.silver.transactions where TransactionDate is null limit 5
# MAGIC
# MAGIC  select * from databricksaml_cat.silver.transactions  limit 5
# MAGIC
# MAGIC  -- drop table databricksaml_cat.silver.transactions 