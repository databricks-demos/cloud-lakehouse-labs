# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Ensuring Governance and security for our C360 lakehouse
# MAGIC 
# MAGIC Data governance and security is hard when it comes to a complete Data Platform. SQL GRANT on tables isn't enough and security must be enforced for multiple data assets (dashboards, Models, files etc).
# MAGIC 
# MAGIC To reduce risks and driving innovation, Emily's team needs to:
# MAGIC 
# MAGIC - Unify all data assets (Tables, Files, ML models, Features, Dashboards, Queries)
# MAGIC - Onboard data with multiple teams
# MAGIC - Share & monetize assets with external Organizations
# MAGIC 
# MAGIC <style>
# MAGIC .box{
# MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
# MAGIC   <div style="width:200px; float: left; text-align: center">
# MAGIC     <div class="box" style="">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team A</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
# MAGIC         Data Analysts<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
# MAGIC         Data Scientists<br/>
# MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
# MAGIC         Data Engineers
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
# MAGIC       <div style="font-size: 26px;">
# MAGIC         <strong>Team B</strong>
# MAGIC       </div>
# MAGIC       <div style="font-size: 13px">...</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on queries, dashboards</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on tables, columns, rows</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on features, ML models, endpoints, notebooks…</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on files, jobs</div>
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
# MAGIC   </div>
# MAGIC   
# MAGIC   <div class="box" style="width:550px; float: left">
# MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
# MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />Governance and Security</div>
# MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
# MAGIC       <ul style="line-height: 2px;">
# MAGIC         <li>Central catalog - all data assets</li>
# MAGIC         <li>Data exploration & discovery to unlock new use-cases</li>
# MAGIC         <li>Permissions cross-teams</li>
# MAGIC         <li>Reduce risk with audit logs</li>
# MAGIC         <li>Measure impact with lineage</li>
# MAGIC       </ul>
# MAGIC       + Monetize & Share data with external organization (Delta Sharing)
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   
# MAGIC   
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Implementing a global data governance and security with Unity Catalog
# MAGIC 
# MAGIC <img style="float: right; margin-top: 30px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-2.png" />
# MAGIC 
# MAGIC Let's see how the Lakehouse can solve this challenge leveraging Unity Catalog.
# MAGIC 
# MAGIC Our Data has been saved as Delta Table by our Data Engineering team.  The next step is to secure this data while allowing cross team to access it. <br>
# MAGIC A typical setup would be the following:
# MAGIC 
# MAGIC * Data Engineers / Jobs can read and update the main data/schemas (ETL part)
# MAGIC * Data Scientists can read the final tables and update their features tables
# MAGIC * Data Analyst have READ access to the Data Engineering and Feature Tables and can ingest/transform additional data in a separate schema.
# MAGIC * Data is masked/anonymized dynamically based on each user access level
# MAGIC 
# MAGIC This is made possible by Unity Catalog. When tables are saved in the Unity Catalog, they can be made accessible to the entire organization, cross-workpsaces and cross users.
# MAGIC 
# MAGIC Unity Catalog is key for data governance, including creating data products or organazing teams around datamesh. It brings among other:
# MAGIC 
# MAGIC * Fined grained ACL
# MAGIC * Audit log
# MAGIC * Data lineage
# MAGIC * Data exploration & discovery
# MAGIC * Sharing data with external organization (Delta Sharing)
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fuc&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exploring our Customer360 database
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
# MAGIC 
# MAGIC Let's review the data created.
# MAGIC 
# MAGIC Unity Catalog works with 3 layers:
# MAGIC 
# MAGIC * CATALOG
# MAGIC * SCHEMA (or DATABASE)
# MAGIC * TABLE
# MAGIC 
# MAGIC All unity catalog is available with SQL (`CREATE CATALOG IF NOT EXISTS my_catalog` ...)
# MAGIC 
# MAGIC To access one table, you can specify the full path: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Let's review the tables we created under our schema
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
# MAGIC 
# MAGIC Unity Catalog provides a comprehensive Data Explorer that you can access on the left menu.
# MAGIC 
# MAGIC You'll find all your tables, and can use it to access and administrate your tables.
# MAGIC 
# MAGIC They'll be able to create extra table into this schema.
# MAGIC 
# MAGIC ### Discoverability 
# MAGIC 
# MAGIC In addition, Unity catalog also provides explorability and discoverability. 
# MAGIC 
# MAGIC Anyone having access to the tables will be able to search it and analyze its main usage. <br>
# MAGIC You can use the Search menu (⌘ + P) to navigate in your data assets (tables, notebooks, queries...)

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

# DBTITLE 1,The above script sets up the default catalog and schema
print("Working catalog and schema: " + labContext.catalogAndSchema())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This lab has been set up to use a specific catalog, if it has been set up, or "main". 
# MAGIC SELECT CURRENT_CATALOG();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows the schemas (databases) in the the current catalog
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows the tables in the current database
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 1,Give access to your schema to other users / or groups
# MAGIC %sql
# MAGIC -- FILL IN <SCHEMA> and <TABLE>
# MAGIC GRANT USE SCHEMA ON SCHEMA <SCHEMA> TO `account users`;
# MAGIC GRANT SELECT ON TABLE <SCHEMA>.<TABLE> TO `account users`;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Going further with Data governance & security
# MAGIC 
# MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
# MAGIC 
# MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
# MAGIC 
# MAGIC ### Fine-grained ACL
# MAGIC 
# MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions.
# MAGIC 
# MAGIC ### Secure external location (S3/ADLS/GCS)
# MAGIC 
# MAGIC Unity Catatalog let you secure your managed table but also your external locations.
# MAGIC 
# MAGIC ### Lineage 
# MAGIC 
# MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level.
# MAGIC 
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC 
# MAGIC 
# MAGIC ### Audit log
# MAGIC 
# MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log.
# MAGIC 
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC 
# MAGIC ### Upgrading to UC
# MAGIC 
# MAGIC Already using Databricks without UC? Upgrading your tables to benefit from Unity Catalog is simple.
# MAGIC 
# MAGIC ### Sharing data with external organization
# MAGIC 
# MAGIC Sharing your data outside of your Databricks users is simple with Delta Sharing, and doesn't require your data consumers to use Databricks.
