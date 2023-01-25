# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Inference - Batch or serverless real-time
# MAGIC 
# MAGIC 
# MAGIC After running AutoML we saved our best model our MLflow registry.
# MAGIC 
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained.
# MAGIC 
# MAGIC This can be done as part of a DLT pipeline or a Workflow in a separate job.
# MAGIC Here is an example to show you how MLflow can be directly used to retrieve the model and run inferences.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC 
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC 
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC 
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC 
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry: Open MLFlow model registry and click the "User model for inference" button!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-4.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
# MAGIC 
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
# MAGIC 
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLflow abstracts all that for us.

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

# DBTITLE 1,Loading the model
import mlflow
#                                                                     Stage/version
#                                                           Model name      |       output
#                                                                |          |         |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/retail_churn/Production", "int")
#We can use the function in SQL
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Creating the final table
model_features = predict_churn_udf.metadata.get_input_schema().input_names()
predictions = spark.table('churn_features').withColumn('churn_prediction', predict_churn_udf(*model_features))
predictions.createOrReplaceTempView("v_churn_prediction")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table churn_prediction as select * from v_churn_prediction

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from churn_prediction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next up
# MAGIC [Explore the data with SQL and create visualisations]($./03 - BI and Data Warehousing)
