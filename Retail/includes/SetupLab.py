# Databricks notebook source
# MAGIC %run ../../includes/CloudLakehouseLabsContext

# COMMAND ----------

labContext = CloudLakehouseLabsContext('retail')
databaseForDLT = labContext.schema() + "_dlt"
rawDataDirectory = labContext.workingDirectory() + '/raw'
deltaTablesDirectory = labContext.workingDirectory() + '/delta_tables'
dltPipelinesOutputDataDirectory = labContext.workingDirectory() + '/dlt_pipelines'
