# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Data engineering with Databricks - Building our C360 database
# MAGIC
# MAGIC Building a C360 database requires to ingest multiple datasources.  
# MAGIC
# MAGIC It's a complex process requiring batch loads and streaming ingestion to support real-time insights, used for personalization and marketing targeting among other.
# MAGIC
# MAGIC Ingesting, transforming and cleaning data to create clean SQL tables for our downstream user (Data Analysts and Data Scientists) is complex.
# MAGIC
# MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
# MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
# MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
# MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
# MAGIC       73%
# MAGIC     </div>
# MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">of enterprise data goes unused for analytics and decision making</div>
# MAGIC   </div>
# MAGIC   <div style="color: #bfbfbf; padding-top: 5px">Source: Forrester</div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John, as Data engineer, spends immense time….
# MAGIC
# MAGIC
# MAGIC * Hand-coding data ingestion & transformations and dealing with technical challenges:<br>
# MAGIC   *Supporting streaming and batch, handling concurrent operations, small files issues, GDPR requirements, complex DAG dependencies...*<br><br>
# MAGIC * Building custom frameworks to enforce quality and tests<br><br>
# MAGIC * Building and maintaining scalable infrastructure, with observability and monitoring<br><br>
# MAGIC * Managing incompatible governance models from different systems
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimatly **putting data projects at risk**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
# MAGIC
# MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-1.png" />
# MAGIC
# MAGIC In this notebook, we'll work as a Data Engineer to build our c360 database. <br>
# MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
# MAGIC
# MAGIC We have 3 data sources sending new files in our blob storage (`/demos/retail/churn/`) and we want to incrementally load this data into our Datawarehousing tables:
# MAGIC
# MAGIC - Customer profile data *(name, age, adress etc)*
# MAGIC - Orders history *(what our customer bough over time)*
# MAGIC - Streaming Events from our application *(when was the last time customers used the application, typically a stream from a Kafka queue)*
# MAGIC
# MAGIC
# MAGIC Databricks simplify this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
# MAGIC
# MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
# MAGIC
# MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC
# MAGIC <div>
# MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
# MAGIC       <strong>Accelerate ETL development</strong> <br/>
# MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
# MAGIC       <strong>Remove operational complexity</strong> <br/>
# MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC     </p>
# MAGIC   </div>
# MAGIC   <div style="width: 48%; float: left">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
# MAGIC       <strong>Trust your data</strong> <br/>
# MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
# MAGIC       <strong>Simplify batch and streaming</strong> <br/>
# MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC     </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <br style="clear:both">
# MAGIC
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
# MAGIC
# MAGIC ## Delta Lake
# MAGIC
# MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. Delta Lake is an open storage framework for reliability and performance.<br>
# MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
# MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fdlt_sql&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Re-building the Data Engineering pipeline with Delta Live Tables
# MAGIC In this example we will re-implement the pipeline we just created using DLT.
# MAGIC
# MAGIC ### Examine the source.
# MAGIC A DLT pipeline can be implemented either in SQL or in Python.
# MAGIC * [DLT pipeline definition in SQL]($./01.2 - Delta Live Tables - SQL)
# MAGIC * [DLT pipeline definition in Python]($./01.2 - Delta Live Tables - Python)
# MAGIC
# MAGIC ### Define the pipeline
# MAGIC Use the UI to achieve that:
# MAGIC * Go to **Workflows / Delta Live Tables / Create Pipeline**
# MAGIC * Specify **churn_data_pipeline** as the name of the pipeline
# MAGIC * As a source specify **one** of the above notebooks. **Either** the SQL or the Python one would work.
# MAGIC * Specify the parameters for the DLT job with the values below:

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

# DBTITLE 1,Parameters for the DLT job
# Create the tables in a database in the hive metastore with data on dbfs
print("Specify the following database/schema when defining the DLT pipeline:\n" + databaseForDLT + "\n")
print("Specify the following storage location for the DLT pipeline tables:\n" + dltPipelinesOutputDataDirectory + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the following after having set up and run the DLT job

# COMMAND ----------

# DBTITLE 1,Count the rows in the churn_features table
sqlStatement = "select count(*) from hive_metastore." + databaseForDLT + ".churn_features"
print("Executing:\n" + sqlStatement)
display(spark.sql(sqlStatement))

# COMMAND ----------

# DBTITLE 1,Retrieve the table details
# Scroll the output to verify the storage location of the table
sqlStatement = "DESCRIBE EXTENDED hive_metastore." + databaseForDLT + ".churn_features"
print("Executing:\n" + sqlStatement)
display(spark.sql(sqlStatement))

# COMMAND ----------

# DBTITLE 1,Retrieve the table history
sqlStatement = "DESCRIBE HISTORY hive_metastore." + databaseForDLT + ".churn_features"
print("Executing:\n" + sqlStatement)
display(spark.sql(sqlStatement))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rerun the DLT pipeline
# MAGIC As not new data are uploaded on the blob storage, there will be only a recalculation of the last table

# COMMAND ----------

# DBTITLE 1,Count the rows in the churn_features table again. It should be the same number
sqlStatement = "select count(*) from hive_metastore." + databaseForDLT + ".churn_features"
print("Executing:\n" + sqlStatement)
display(spark.sql(sqlStatement))

# COMMAND ----------

# DBTITLE 1,Retrieve the table history. There is an additional entry now
sqlStatement = "DESCRIBE HISTORY hive_metastore." + databaseForDLT + ".churn_features"
print("Executing:\n" + sqlStatement)
display(spark.sql(sqlStatement))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next up
# MAGIC [Build and train a Machine Learning model]($./02 - Machine Learning with MLflow)
