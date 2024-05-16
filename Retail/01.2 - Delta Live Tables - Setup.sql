-- Databricks notebook source
-- MAGIC %run ./includes/SetupLab

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Create the tables in a database Unity Catalog with data on dbfs
-- MAGIC print("Specify the following database/schema when defining the DLT pipeline:\n" + databaseForDLT + "\n")
-- MAGIC
