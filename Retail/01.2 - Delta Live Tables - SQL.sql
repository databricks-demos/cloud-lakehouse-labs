-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
-- MAGIC
-- MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-1.png" />
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our c360 database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC We have 3 data sources sending new files in our blob storage (`/demos/retail/churn/`) and we want to incrementally load this data into our Datawarehousing tables:
-- MAGIC
-- MAGIC - Customer profile data *(name, age, adress etc)*
-- MAGIC - Orders history *(what our customer bough over time)*
-- MAGIC - Streaming Events from our application *(when was the last time customers used the application, typically a stream from a Kafka queue)*
-- MAGIC
-- MAGIC
-- MAGIC Databricks simplify this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
-- MAGIC
-- MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Accelerate ETL development</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Remove operational complexity</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Simplify batch and streaming</strong> <br/>
-- MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fdlt_sql&dt=LAKEHOUSE_RETAIL_CHURN">
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob cloud storage. 

-- COMMAND ----------

-- DBTITLE 1,Ingest raw app events stream in incremental mode
CREATE STREAMING LIVE TABLE churn_app_events (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Application events and sessions"
AS SELECT * FROM cloud_files("/Volumes/main/odl_instructor_1306122_databrickslabs_com_retail/retail/events", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Ingest raw orders from ERP
CREATE STREAMING LIVE TABLE churn_orders_bronze (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Spending score from raw data"
AS SELECT * FROM cloud_files("/Volumes/main/odl_instructor_1306122_databrickslabs_com_retail/retail/orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE STREAMING LIVE TABLE churn_users_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/Volumes/main/odl_instructor_1306122_databrickslabs_com_retail/retail/users", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Clean and anonymise User data
CREATE STREAMING LIVE TABLE churn_users (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "id")
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  id as user_id,
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  initcap(firstname) as firstname, 
  initcap(lastname) as lastname, 
  address, 
  channel, 
  country,
  cast(gender as int),
  cast(age_group as int), 
  cast(churn as int) as churn
from STREAM(live.churn_users_bronze)

-- COMMAND ----------

-- DBTITLE 1,Clean orders
CREATE STREAMING LIVE TABLE churn_orders (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, 
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date

from STREAM(live.churn_orders_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our ML features
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC We're now ready to create the features required for our Churn prediction.
-- MAGIC
-- MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
-- MAGIC
-- MAGIC * last command date
-- MAGIC * number of item bought
-- MAGIC * number of actions in our website
-- MAGIC * device used (ios/iphone)
-- MAGIC * ...

-- COMMAND ----------

-- DBTITLE 1,Create the feature table
CREATE LIVE TABLE churn_features
COMMENT "Final user table with all information for Analysis / ML"
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders GROUP BY user_id),  
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
        FROM live.churn_app_events GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
       FROM live.churn_users
         INNER JOIN churn_orders_stats using (user_id)
         INNER JOIN churn_app_events_stats using (user_id)