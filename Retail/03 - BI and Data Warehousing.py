# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Your Lakehouse is the best Warehouse
# MAGIC 
# MAGIC Traditional Data Warehouses can’t keep up with the variety of data and use cases. Business agility requires reliable, real-time data, with insight from ML models.
# MAGIC 
# MAGIC Working with the lakehouse unlock traditional BI analysis but also real time applications having a direct connection to your entire data, while remaining fully secured.
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
# MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
# MAGIC   Instant, elastic compute<br>
# MAGIC   Lower TCO with Serveless<br>
# MAGIC   Zero management<br>
# MAGIC   Governance layer - row level<br>
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

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

print("For the following exercise use the following catalog.schema : \n" + labContext.catalogAndSchema() )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lab exercise
# MAGIC 
# MAGIC Create the following queries and visualisations using the above catalog and schema
# MAGIC 
# MAGIC **1. Total MRR**
# MAGIC ```
# MAGIC SELECT
# MAGIC   sum(amount)/1000 as MRR
# MAGIC FROM churn_orders
# MAGIC WHERE
# MAGIC 	month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')) = 
# MAGIC   (
# MAGIC     select max(month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')))
# MAGIC   	from churn_orders
# MAGIC   );
# MAGIC ```
# MAGIC Create a *counter* visualisation
# MAGIC 
# MAGIC **2. MRR at Risk**
# MAGIC ```
# MAGIC SELECT
# MAGIC 	sum(amount)/1000 as MRR_at_risk
# MAGIC FROM churn_orders
# MAGIC WHERE month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')) = 
# MAGIC 	(
# MAGIC 		select max(month(to_timestamp(transaction_date, 'MM-dd-yyyy HH:mm:ss')))
# MAGIC 		from churn_orders
# MAGIC 	)
# MAGIC 	and user_id in
# MAGIC 	(
# MAGIC 		SELECT user_id FROM churn_prediction WHERE churn_prediction=1
# MAGIC 	)
# MAGIC ```
# MAGIC Create a *counter* visualisation
# MAGIC 
# MAGIC **3. Customers at risk**
# MAGIC ```
# MAGIC SELECT count(*) as Customers, cast(churn_prediction as boolean) as `At Risk`
# MAGIC FROM churn_prediction GROUP BY churn_prediction;
# MAGIC ```
# MAGIC 
# MAGIC ### For 4 and 5 switch to the schema in the hive metastore where the DLT tables are created 
# MAGIC 
# MAGIC **4. Customer Tenure - Historical**
# MAGIC ```
# MAGIC SELECT cast(days_since_creation/30 as int) as days_since_creation, churn, count(*) as customers
# MAGIC FROM churn_features
# MAGIC GROUP BY days_since_creation, churn having days_since_creation < 1000
# MAGIC ```
# MAGIC Create a *bar* visualisation
# MAGIC 
# MAGIC **5. Subscriptions by Internet Service - Historical**
# MAGIC ```
# MAGIC select platform, churn, count(*) as event_count
# MAGIC from churn_app_events
# MAGIC inner join churn_users using (user_id)
# MAGIC where platform is not null
# MAGIC group by platform, churn
# MAGIC ```
# MAGIC Create a *horizontal bar* visualisation
# MAGIC 
# MAGIC ### For the rest switch back to the original catalog and schema (if applicable) 
# MAGIC 
# MAGIC **6. Predicted to churn by channel**
# MAGIC ```
# MAGIC SELECT channel, count(*) as users
# MAGIC FROM churn_prediction
# MAGIC WHERE churn_prediction=1 and channel is not null
# MAGIC GROUP BY channel
# MAGIC ```
# MAGIC Create a *pie chart* visualisation
# MAGIC 
# MAGIC **7. Predicted to churn by country**
# MAGIC ```
# MAGIC SELECT country, churn_prediction, count(*) as customers
# MAGIC FROM churn_prediction
# MAGIC GROUP BY country, churn_prediction
# MAGIC ```
# MAGIC Create a *bar* visualisation

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Creating our Churn Dashboard
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" />
# MAGIC 
# MAGIC The next step is now to assemble our queries and their visualization in a comprehensive SQL dashboard that our business will be able to track.
# MAGIC 
# MAGIC ### Lab exercise
# MAGIC Assemple the visualisations defined with the above queries into a dashboard

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Using Third party BI tools
# MAGIC 
# MAGIC <iframe style="float: right" width="560" height="315" src="https://www.youtube.com/embed/EcKqQV0rCnQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
# MAGIC 
# MAGIC SQL warehouse can also be used with an external BI tool such as Tableau or PowerBI.
# MAGIC 
# MAGIC This will allow you to run direct queries on top of your table, with a unified security model and Unity Catalog (ex: through SSO). Now analysts can use their favorite tools to discover new business insights on the most complete and freshest data.
# MAGIC 
# MAGIC To start using your Warehouse with third party BI tool, click on "Partner Connect" on the bottom left and chose your provider.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Going further with DBSQL & Databricks Warehouse
# MAGIC 
# MAGIC Databricks SQL offers much more and provides a full warehouse capabilities
# MAGIC 
# MAGIC <img style="float: right" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-pk-fk.png" />
# MAGIC 
# MAGIC ### Data modeling
# MAGIC 
# MAGIC Comprehensive data modeling. Save your data based on your requirements: Data vault, Star schema, Inmon...
# MAGIC 
# MAGIC Databricks let you create your PK/FK, identity columns (auto-increment)
# MAGIC 
# MAGIC ### Data ingestion made easy with DBSQL & DBT
# MAGIC 
# MAGIC Turnkey capabilities allow analysts and analytic engineers to easily ingest data from anything like cloud storage to enterprise applications such as Salesforce, Google Analytics, or Marketo using Fivetran. It’s just one click away. 
# MAGIC 
# MAGIC Then, simply manage dependencies and transform data in-place with built-in ETL capabilities on the Lakehouse (Delta Live Table), or using your favorite tools like dbt on Databricks SQL for best-in-class performance.
# MAGIC 
# MAGIC ### Query federation
# MAGIC 
# MAGIC Need to access cross-system data? Databricks SQL query federation let you define datasources outside of databricks (ex: PostgreSQL)
# MAGIC 
# MAGIC ### Materialized views
# MAGIC 
# MAGIC Avoid expensive queries and materialize your tables. The engine will recompute only what's required when your data get updated. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next up
# MAGIC [Orchestrating and automating with Workflows]($./04 - Orchestrating with Workflows)
