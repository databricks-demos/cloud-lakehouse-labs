# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Data Science with Databricks
# MAGIC 
# MAGIC ## ML is key to disruption & personalization
# MAGIC 
# MAGIC Being able to ingest and query our C360 database is a first step, but this isn't enough to thrive in a very competitive market.
# MAGIC 
# MAGIC Customers now expect real time personalization and new form of comunication. Modern data company achieve this with AI.
# MAGIC 
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 500px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:80px">90%</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC       Enterprise applications will be AI-augmented by 2025 —IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">$10T+</div>
# MAGIC     <div style="font-size:30px;  margin-top: -20px; line-height: 30px;">
# MAGIC        Projected business value creation by AI in 2030 —PWC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC   <div class="right_box">
# MAGIC       But—huge challenges getting ML to work at scale!<br/><br/>
# MAGIC       Most ML projects still fail before getting to production
# MAGIC   </div>
# MAGIC   
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC ## Machine learning is data + transforms.
# MAGIC 
# MAGIC ML is hard because delivering value to business lines isn't only about building a Model. <br>
# MAGIC The ML lifecycle is made of data pipelines: Data-preprocessing, feature engineering, training, inference, monitoring and retraining...<br>
# MAGIC Stepping back, all pipelines are data + code.
# MAGIC 
# MAGIC 
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-4.png" />
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px">Marc, as a Data Scientist, needs a data + ML platform accelerating all the ML & DS steps:</h3>
# MAGIC 
# MAGIC <div style="font-size: 19px; margin-left: 73px; clear: left">
# MAGIC <div class="badge_b"><div class="badge">1</div> Build Data Pipeline supporting real time (with DLT)</div>
# MAGIC <div class="badge_b"><div class="badge">2</div> Data Exploration</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> Feature creation</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> Build & train model</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> Deploy Model (Batch or serverless realtime)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> Monitoring</div>
# MAGIC </div>
# MAGIC 
# MAGIC **Marc needs A Lakehouse**. Let's see how we can deploy a Churn model in production within the Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Goal Of Machine Learning vs Traditional Software Development
# MAGIC <div><img src="https://pages.databricks.com/rs/094-YMS-629/images/mlflowneed.png" width="850"></div>
# MAGIC 
# MAGIC ###MLflow Components
# MAGIC 
# MAGIC <div><img src="https://pages.databricks.com/rs/094-YMS-629/images/mlflowcomponents.png" width="850"></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning lifecycle, data scientists test many different models from various libraries with different hyperparemeters.  Tracking these various results poses an organizational challenge.  In brief, storing experiements, results, models, supplementary artifacts, and code creates significant challenges in the machine learning lifecycle.
# MAGIC 
# MAGIC MLflow Tracking is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiement
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.
# MAGIC 
# MAGIC <div><img src="https://pages.databricks.com/rs/094-YMS-629/images/mlflow-tracking.png" style="height: 300px; margin: 20px"/></div>
# MAGIC <div><img src="https://pages.databricks.com/rs/094-YMS-629/images/3 - Unify data and ML across the full lifecycle.png" width="950"></div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Building a Churn Prediction Model
# MAGIC 
# MAGIC Let's see how we can now leverage the C360 data to build a model predicting and explaining customer Churn.
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-churn-ds-flow.png" width="1000px">
# MAGIC 
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*

# COMMAND ----------

# MAGIC %run ./includes/SetupLab

# COMMAND ----------

# MAGIC %md
# MAGIC ### Our training Data
# MAGIC The tables generated with the DLT pipeline contain a **churn** flag which will be used as the label for training of the model.
# MAGIC The predictions will eventually be applied to the tables generated with the spark pipeline.

# COMMAND ----------

# DBTITLE 1,For the time being the Feature Score is not fully integrated with Unity Catalog
spark.sql('USE CATALOG hive_metastore')
spark.sql(f'USE {databaseForDLT}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC 
# MAGIC Let's review our dataset and start analyze the data we have to predict our churn

# COMMAND ----------

# DBTITLE 1,Read our churn gold table
# Read our churn_features table
churn_dataset = spark.table("churn_features")
display(churn_dataset)

# COMMAND ----------

# DBTITLE 1,Data Exploration and analysis
import seaborn as sns
g = sns.PairGrid(churn_dataset.sample(0.01).toPandas()[['age_group','gender','order_count']], diag_sharey=False)
g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Further data analysis and preparation using pandas API
# MAGIC 
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use `pandas on spark` to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC 
# MAGIC Typicaly a Data Science project would involve more a advanced preparation and likely require extra data prep steps, including more a complex feature preparation.

# COMMAND ----------

# DBTITLE 1,Custom pandas transformation / code on top of your entire dataset
# Convert to pandas on spark
dataset = churn_dataset.pandas_api()
dataset.describe()  
# Drop columns we don't want to use in our model
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'last_event'])
# Drop missing values
dataset = dataset.dropna()
# print the ten first rows
dataset[:10]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Write to Feature Store
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC Once our features are ready, we can save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC 
# MAGIC This will allow discoverability and reusability of our feature across our organization, increasing team efficiency.
# MAGIC 
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features. It also simplify realtime serving.
# MAGIC 
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table('churn_user_features')
except: pass

#Note: You might need to delete the FS table using the UI
churn_feature_table = fs.create_table(
  name='churn_user_features',
  primary_keys='user_id',
  schema=dataset.spark.schema(),
  description='These features are derived from the churn_bronze_customers table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)

fs.write_table(df=dataset.to_spark(), name='churn_user_features', mode='overwrite')
features = fs.read_table('churn_user_features')
display(features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training a model from the table in the Feature Store
# MAGIC 
# MAGIC As we will be using a scikit-learn algorith, we will convert the feature table into a pandas model

# COMMAND ----------

# Convert to Pandas
df = features.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train - test splitting

# COMMAND ----------

# Split to train and test set
from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define the preprocessing steps

# COMMAND ----------

# Select the columns
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
supported_cols = ["event_count", "gender", "total_amount", "country", "order_count", "channel", "total_item", "days_since_last_activity", "days_last_event", "days_since_creation", "session_count", "age_group", "platform"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# Preprocessing
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["age_group", "days_last_event", "days_since_creation", "days_since_last_activity", "event_count", "gender", "order_count", "session_count", "total_amount", "total_item"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["event_count", "gender", "total_amount", "order_count", "total_item", "days_since_last_activity", "days_last_event", "days_since_creation", "session_count", "age_group"])]

# COMMAND ----------

# Treating categorical variables
from databricks.automl_runtime.sklearn import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="indicator")),
])
categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["age_group", "channel", "country", "event_count", "gender", "order_count", "platform", "session_count"])]

# COMMAND ----------

# Final transformation of the columns
from sklearn.compose import ColumnTransformer
transformers = numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# Separate target column from features
target_col = "churn"
X_train = train_df.drop([target_col], axis=1)
y_train = train_df[target_col]

X_test = test_df.drop([target_col], axis=1)
y_test = test_df[target_col]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training a model and logging everything with MLflow

# COMMAND ----------

# DBTITLE 0,Train a model and log it with MLflow
import pandas as pd
import mlflow
from mlflow.models import Model
from mlflow import pyfunc
from mlflow.pyfunc import PyFuncModel

import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline

# Start a run
with mlflow.start_run(run_name="simple-RF-run") as run:
  classifier = RandomForestClassifier()
  model = Pipeline([
      ("column_selector", col_selector),
      ("preprocessor", preprocessor),
      ("classifier", classifier),
  ])

  # Enable automatic logging of input samples, metrics, parameters, and models
  mlflow.sklearn.autolog(
      log_input_examples=True,
      silent=True)

  model.fit(X_train, y_train)

  # Log metrics for the test set
  mlflow_model = Model()
  pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
  pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
  X_test[target_col] = y_test
  test_eval_result = mlflow.evaluate(
      model=pyfunc_model,
      data=X_test,
      targets=target_col,
      model_type="classifier",
      evaluator_config = {"log_model_explainability": False,
                          "metric_prefix": "test_" , "pos_label": 1 }
  )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Explore the above in the UI
# MAGIC 
# MAGIC From the experiments page select the "02 - Machine Learning with MLflow" experiment and see the associated runs

# COMMAND ----------

# MAGIC %md-sandbox <i18n value="5802ff47-58b5-4789-973d-2fb855bf347a"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Model Registry
# MAGIC 
# MAGIC The MLflow Model Registry component is a centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of an MLflow Model. It provides model lineage (which MLflow Experiment and Run produced the model), model versioning, stage transitions (e.g. from staging to production), annotations (e.g. with comments, tags), and deployment management (e.g. which production jobs have requested a specific model version).
# MAGIC 
# MAGIC Model registry has the following features:<br><br>
# MAGIC 
# MAGIC * **Central Repository:** Register MLflow models with the MLflow Model Registry. A registered model has a unique name, version, stage, and other metadata.
# MAGIC * **Model Versioning:** Automatically keep track of versions for registered models when updated.
# MAGIC * **Model Stage:** Assigned preset or custom stages to each model version, like “Staging” and “Production” to represent the lifecycle of a model.
# MAGIC * **Model Stage Transitions:** Record new registration events or changes as activities that automatically log users, changes, and additional metadata such as comments.
# MAGIC * **CI/CD Workflow Integration:** Record stage transitions, request, review and approve changes as part of CI/CD pipelines for better control and governance.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/model-registry.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> See <a href="https://mlflow.org/docs/latest/registry.html" target="_blank">the MLflow docs</a> for more details on the model registry.

# COMMAND ----------

# DBTITLE 1,Register the model in the model registry
from mlflow.tracking.client import MlflowClient

logged_model = 'runs:/' + run.info.run_id + '/model'

print("Registeting the model under the name '" + modelName + "'")
result=mlflow.register_model(logged_model, modelName, await_registration_for=0)

# COMMAND ----------

# DBTITLE 1,Retrieving the model status and moving it to the "Staging" stage
import time

# Retrieving the model
client = MlflowClient()
model_version_details = None
while True:
  model_version_details = client.get_model_version(name=modelName, version=result.version)
  if model_version_details.status == 'READY': break
  time.sleep(5)

client.transition_model_version_stage(
    name=model_version_details.name,
    version=model_version_details.version,
    stage="Production",
    archive_existing_versions = True
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Accelerating Churn model creation using MLFlow and Databricks Auto-ML
# MAGIC 
# MAGIC MLflow is an open source project allowing model tracking, packaging and deployment. Everytime your datascientist team work on a model, Databricks will track all the parameter and data used and will save it. This ensure ML traceability and reproductibility, making it easy to know which model was build using which parameters/data.
# MAGIC 
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC While Databricks simplify model deployment and governance (MLOps) with MLFlow, bootstraping new ML projects can still be long and inefficient. 
# MAGIC 
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC 
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC 
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`churn_features`)
# MAGIC 
# MAGIC Our prediction target is the `churn` column.
# MAGIC 
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab exercise - AutoML
# MAGIC 
# MAGIC Let's create a better model with just a few clicks!
# MAGIC * Create an AutoML experiment
# MAGIC * register the best run with the model named as above.
# MAGIC * Promote the new version to **Production**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next up
# MAGIC [Use the model to predict the churn]($./02.1 - Machine Learning - Inference)
