# Databricks notebook source
# MAGIC %md
# MAGIC ## Let's start with a business problem:
# MAGIC
# MAGIC ## *Building a Customer 360 database and reducing customer churn with the Databricks Lakehouse*
# MAGIC
# MAGIC In this demo, we'll step in the shoes of a retail company selling goods with a recurring business.
# MAGIC
# MAGIC The business has determined that the focus must be placed on churn. We're asked to:
# MAGIC
# MAGIC * Analyse and explain current customer churn: quantify churn, trends and the impact for the business
# MAGIC * Build a proactive system to forecast and reduce churn by taking automated action: targeted email, phoning etc.
# MAGIC
# MAGIC
# MAGIC ### What we'll build
# MAGIC
# MAGIC To do so, we'll build an end-to-end solution with the Lakehouse. To be able to properly analyse and predict our customer churn, we need information coming from different external systems: Customer profiles coming from our website, order details from our ERP system and mobile application clickstream to analyse our customers activity.
# MAGIC
# MAGIC At a very high level, this is the flow we'll implement:
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-0.png" />
# MAGIC
# MAGIC 1. Ingest and create our Customer 360 database, with tables easy to query in SQL
# MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
# MAGIC 3. Run BI queries to analyse existing churn
# MAGIC 4. Build ML model to predict which customer is going to churn and why
# MAGIC
# MAGIC As a result, we will have all the information required to trigger custom actions to increase retention (email personalized, special offers, phone call...)
# MAGIC
# MAGIC ### Our dataset
# MAGIC
# MAGIC For simplicity, we will assume that an external system is periodically sending data into our blob cloud storage:
# MAGIC
# MAGIC - Customer profile data *(name, age, address etc)*
# MAGIC - Orders history *(what ours customers have bought over time)*
# MAGIC - Events from our application *(when was the last time a customer used the application, what clics were recorded, typically collected through a stream)*
# MAGIC
# MAGIC *Note that at our data could be arriving from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Raw data generation
# MAGIC
# MAGIC For this demonstration we will not be using real data or an existing dataset, but will rather generate them.
# MAGIC
# MAGIC The cell below will execute a notebook that will generate the data and store them on DBFS.
# MAGIC If you want to see the actual code [click here to open it on a different tab]($./includes/CreateRawData)

# COMMAND ----------

# MAGIC %run ./includes/CreateRawData

# COMMAND ----------

# DBTITLE 1,The raw data on DBFS
ordersFolder = rawDataDirectory + '/orders'
usersFolder = rawDataDirectory + '/users'
eventsFolder = rawDataDirectory + '/events'
print('Order raw data stored under the DBFS folder "' + ordersFolder + '"')
print('User raw data stored under the DBFS folder "' + usersFolder + '"')
print('Website event raw data stored under the DBFS folder "' + eventsFolder + '"')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What we are going to implement
# MAGIC
# MAGIC We will initially load the raw data with the autoloader,
# MAGIC perform some cleaning and enrichment operations,
# MAGIC develop and load a model from MLFlow to predict our customer churn,
# MAGIC and finally use this information to build our DBSQL dashboard to track customer behavior and churn.
# MAGIC  
# MAGIC <div><img width="1100px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta.png"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's start with
# MAGIC [Data Engineering with Delta]($./01 - Data Engineering with Delta)
