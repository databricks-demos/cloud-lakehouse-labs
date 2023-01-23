# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Setting up the Lab with the specific use case

# COMMAND ----------

# MAGIC %run ../../includes/CloudLakehouseLabsContext

# COMMAND ----------

labContext = CloudLakehouseLabsContext('retail')
databaseForDLT = labContext.schema() + "_dlt"
rawDataDirectory = labContext.workingDirectory() + '/raw'
deltaTablesDirectory = labContext.workingDirectory() + '/delta_tables'
dltPipelinesOutputDataDirectory = labContext.workingDirectory() + '/dlt_pipelines'
