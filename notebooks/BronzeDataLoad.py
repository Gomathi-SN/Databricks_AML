# Databricks notebook source
# MAGIC %md
# MAGIC Load **Transaction** using Autoloader and write the stream to Bronze schema 

# COMMAND ----------

df_tran= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("cloudFiles.schemaHints","amount double, Time TIMESTAMP") \
    .option("cloudFiles.schemaLocation","abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/checkpoint/transactions")\
    .load("abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/Transactions/")



# COMMAND ----------

df_tran.writeStream.format("delta")\
          .outputMode("append")\
        .option("checkpointLocation","abfss://bronze@databricksamlstoracc.dfs.core.windows.net/checkpoint/transactions/")\
        .trigger(once=True)\
    .toTable("databricksaml_cat.bronze.transactions")


# COMMAND ----------

# MAGIC %md
# MAGIC **Amount** field is not loaded into column. The values are present in _rescued_data column
# MAGIC Data will be retrieved 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Amount , cast( _rescued_data:Amount as double) from databricksaml_cat.bronze.transactions
# MAGIC
# MAGIC  select *  from databricksaml_cat.bronze.transactions where Sender_account = 9665544655  and Receiver_account = 5786322615
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Clients data**

# COMMAND ----------

df_clients=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("header", "true")\
    .option("cloudFiles.schemaLocation","abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/checkpoint/clients")\
    .load("abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/clients/")



# COMMAND ----------

df_clients.writeStream.format("delta")\
       .outputMode("append")\
       .option("checkpointLocation","abfss://bronze@databricksamlstoracc.dfs.core.windows.net/checkpoint/clients/")\
        .trigger(once=True)\
    .toTable("databricksaml_cat.bronze.clients")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from databricksaml_cat.bronze.clients
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Load **Accounts **details for all clients

# COMMAND ----------

df_accounts= spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("cloudFiles.schemaLocation","abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/checkpoint/accounts")\
    .load("abfss://sourcedata@databricksamlstoracc.dfs.core.windows.net/accounts/")


# COMMAND ----------

df_accounts.writeStream.format("delta")\
     .outputMode("append")\
    .option("checkpointLocation","abfss://bronze@databricksamlstoracc.dfs.core.windows.net/checkpoint/accounts/")\
        .trigger(once=True)\
    .toTable("databricksaml_cat.bronze.accounts")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from databricksaml_cat.bronze.accounts 
# MAGIC