# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricksaml_cat.gold.PEPCustomerAlert AS
# MAGIC select distinct CL.client_id, CL.client_name, CL.sector, CL.sector_risk, CL.country
# MAGIC from databricksaml_cat.silver.clients CL
# MAGIC join databricksaml_cat.silver.accounts AC on CL.client_id = AC.client_id
# MAGIC join ( select distinct Sender_account from databricksaml_cat.gold.highvoltrans ) HV on AC.sender_account = HV.Sender_account
# MAGIC where pep_flag='1'