# Databricks notebook source
# Helper class that captures the execution context

import unicodedata
import re

class CloudLakehouseLabsContext:
  def __init__(self, useCase: str):
    self.__useCase = useCase
    self.__cloud = spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()
    self.__user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    text = self.__user
    try: text = unicode(text, 'utf-8')
    except (TypeError, NameError): pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode("utf-8").lower()
    self.__user_id = re.sub("[^a-zA-Z0-9]", "_", text)
    self.__volumeName = useCase

    # Create the working schema
    catalogName = None
    databaseName = self.__user_id + '_' + self.__useCase
    volumeName = self.__volumeName
    for catalog in ['cloud_lakehouse_labs', 'main', 'dbdemos', 'hive_metastore']:
      try:
        catalogName = catalog
        if catalogName != 'hive_metastore':
          self.__catalog = catalogName
          spark.sql("create database if not exists " + catalog + "." + databaseName)
          spark.sql("CREATE VOLUME " + catalog + "." + databaseName + "." + volumeName)
        else: 
          self.__catalog = catalogName
          spark.sql("create database if not exists " + databaseName)
        break
      except Exception as e:
        pass
    if catalogName is None: raise Exception("No catalog found with CREATE SCHEMA privileges for user '" + self.__user + "'")
    self.__schema = databaseName
    if catalogName != 'hive_metastore': spark.sql('use catalog ' + self.__catalog)
    spark.sql('use database ' + self.__schema)

    # Create the working directory under DBFS
    self.__workingDirectory = '/Users/' + self.__user_id + '/' + self.__useCase
    dbutils.fs.mkdirs(self.__workingDirectory)

  def cloud(self): return self.__cloud

  def user(self): return self.__user

  def schema(self): return self.__schema

  def volumeName(self): return self.volumeName

  def catalog(self): return self.__catalog

  def catalogAndSchema(self): return self.__catalog + '.' + self.__schema

  def workingDirectory(self): return self.__workingDirectory

  def workingVolumeDirectory(self): return "/Volumes/main/"+self.__schema+"/"+self.__volumeName

  def useCase(self): return self.__useCase

  def userId(self): return self.__user_id

  def dropAllDataAndSchema(self):
    try:
      spark.sql('DROP DATABASE IF EXISTS ' + self.catalogAndSchema() + ' CASCADE')
    except Exception as e:
      print(str(e))
    try:
      dbutils.fs.rm(self.__workingDirectory, recurse=True)
    except Exception as e:
      print(str(e))