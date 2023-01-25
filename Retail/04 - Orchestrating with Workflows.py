# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Orchestrating our Churn pipeline with Databricks Workflows
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/05/workflows-orchestrate-img.png" />
# MAGIC 
# MAGIC With Databricks Lakehouse, no need for external orchestrator. We can use [Workflows](/#job/list) (available on the left menu) to orchestrate our Churn pipeline within a few click.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ###  Orchestrate anything anywhere
# MAGIC With workflow, you can run diverse workloads for the full data and AI lifecycle on any cloud. Orchestrate Delta Live Tables and Jobs for SQL, Spark, notebooks, dbt, ML models and more.
# MAGIC 
# MAGIC ### Simple - Fully managed
# MAGIC Remove operational overhead with a fully managed orchestration service, so you can focus on your workflows not on managing your infrastructure.
# MAGIC 
# MAGIC ### Proven reliability
# MAGIC Have full confidence in your workflows leveraging our proven experience running tens of millions of production workloads daily across clouds.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Creating your workflow
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow.png" />
# MAGIC 
# MAGIC A Databricks Workflow is composed of Tasks.
# MAGIC 
# MAGIC Each task can trigger a specific job:
# MAGIC 
# MAGIC * Delta Live Tables
# MAGIC * SQL query / dashboard
# MAGIC * Model retraining / inference
# MAGIC * Notebooks
# MAGIC * dbt
# MAGIC * ...
# MAGIC 
# MAGIC In this example, can see our 3 tasks:
# MAGIC 
# MAGIC * Start the DLT pipeline to ingest new data and refresh our tables
# MAGIC * Refresh the DBSQL dashboard (and potentially notify downstream applications)
# MAGIC * Retrain our Churn model

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Monitoring your runs
# MAGIC 
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-workflow-monitoring.png" />
# MAGIC 
# MAGIC Once your workflow is created, we can access historical runs and receive alerts if something goes wrong!
# MAGIC 
# MAGIC In the screenshot we can see that our workflow had multiple errors, with different runtime, and ultimately got fixed.
# MAGIC 
# MAGIC Workflow monitoring includes errors, abnormal job duration and more advanced control!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab exercise - Create a Workflow
# MAGIC 
# MAGIC From the Workflows page **Create a New Job** with the following tasks
# MAGIC * **1. Ingest_and_process_new_data**
# MAGIC Use the notebook [01 - Data Engineering with Delta]($./01 - Data Engineering with Delta) as the task source
# MAGIC * **2. Create_Predictions**
# MAGIC Use the notebook [02.1 - Machine Learning - Inference]($./02.1 - Machine Learning - Inference) as the task source<br>
# MAGIC *Important:* for this task create a new job cluster that runs on an ML-enabled runtime!
# MAGIC * **3. Refresh_Dashboard**
# MAGIC Specify **SQL** for the task type and **Dashboard** as the SQL task.
# MAGIC Select the dashboard you created in the previous step as well as an existing SQL Warehouse
# MAGIC 
# MAGIC Save and **Run now**

# COMMAND ----------

# MAGIC %md
# MAGIC #Congratulations!
# MAGIC You have reached the end of this lab and learned how to **create business value** in record time thanks to the **Databricks Lakehouse.**
