# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Your Lakehouse is the best Warehouse
# MAGIC 
# MAGIC Traditional Data Warehouses can’t keep up with the variety of data and use cases. Business agility requires reliable, real-time data, with insight from ML models.
# MAGIC 
# MAGIC Working with the lakehouse unlock traditional BI analysis but also real time applications having a direct connection to your entire data, while remaining fully secured.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
# MAGIC 
# MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
# MAGIC   Instant, elastic compute<br>
# MAGIC   Lower TCO with Serveless<br>
# MAGIC   Zero management<br><br>
# MAGIC 
# MAGIC   Governance layer - row level<br><br>
# MAGIC 
# MAGIC   Your data. Your schema (star, data vault…)
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # BI & Datawarehousing with Databricks SQL
# MAGIC 
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-3.png" />
# MAGIC 
# MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
# MAGIC 
# MAGIC Let's explore how Databricks SQL support your Data Analyst team with interactive BI and start analyzing our customer Churn.
# MAGIC 
# MAGIC To start with Databricks SQL, open the SQL view on the top left menu.
# MAGIC 
# MAGIC You'll be able to:
# MAGIC 
# MAGIC - Create a SQL Warehouse to run your queries
# MAGIC - Use DBSQL to build your own dashboards
# MAGIC - Plug any BI tools (Tableau/PowerBI/..) to run your analysis
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fbi&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks SQL Warehouses: best-in-class BI engine
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
# MAGIC 
# MAGIC Databricks SQL is a warehouse engine packed with thousands of optimizations to provide you with the best performance for all your tools, query types and real-world applications. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>It holds the Data Warehousing Performance Record.</a>
# MAGIC 
# MAGIC This includes the next-generation vectorized query engine Photon, which together with SQL warehouses, provides up to 12x better price/performance than other cloud data warehouses.
# MAGIC 
# MAGIC **Serverless warehouse** provide instant, elastic SQL compute — decoupled from storage — and will automatically scale to provide unlimited concurrency without disruption, for high concurrency use cases.
# MAGIC 
# MAGIC Make no compromise. Your best Datawarehouse is a Lakehouse.
# MAGIC 
# MAGIC ### Creating a SQL Warehouse
# MAGIC 
# MAGIC SQL Wharehouse are managed by databricks. [Creating a warehouse](/sql/warehouses) is a 1-click step: 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Creating your first Query
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
# MAGIC 
# MAGIC Our users can now start running SQL queries using the SQL editor and add new visualizations.
# MAGIC 
# MAGIC By leveraging auto-completion and the schema browser, we can start running adhoc queries on top of our data.
# MAGIC 
# MAGIC While this is ideal for Data Analyst to start analysing our customer Churn, other personas can also leverage DBSQL to track our data ingestion pipeline, the data quality, model behavior etc.
# MAGIC 
# MAGIC Open the [Queries menu](/sql/queries) to start writting your first analysis.

# COMMAND ----------


