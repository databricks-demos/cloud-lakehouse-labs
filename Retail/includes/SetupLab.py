# Databricks notebook source
# MAGIC %run ../../includes/CloudLakehouseLabsContext

# COMMAND ----------

class RetailCloudLakehouseLabsContext(CloudLakehouseLabsContext):
  def __init__(self):
    super().__init__('retail')
    self.__databaseForDLT = self.schema() + "_dlt"
    self.__rawDataDirectory = "/cloud_lakehouse_labs/retail/raw"
    self.__deltaTablesDirectory = self.workingDirectory() + "/delta_tables"
    self.__dltPipelinesOutputDataDirectory = self.workingDirectory() + "/dlt_pipelines"

  def dropAllDataAndSchema(self):
    super().dropAllDataAndSchema()
    try:
      spark.sql('DROP DATABASE IF EXISTS hive_metastore.' + self.__databaseForDLT + ' CASCADE')
    except Exception as e:
      pass


  def databaseForDLT(self): return self.__databaseForDLT
  def rawDataDirectory(self): return self.__rawDataDirectory
  def deltaTablesDirectory(self): return self.__deltaTablesDirectory
  def dltPipelinesOutputDataDirectory(self): return self.__dltPipelinesOutputDataDirectory
  def modelNameForUser(self): return "retail_churn_" + self.userId()

# COMMAND ----------

labContext = RetailCloudLakehouseLabsContext()
databaseForDLT = labContext.databaseForDLT()
rawDataDirectory = labContext.rawDataDirectory()
deltaTablesDirectory = labContext.deltaTablesDirectory()
dltPipelinesOutputDataDirectory = labContext.dltPipelinesOutputDataDirectory()
modelName = labContext.modelNameForUser()
