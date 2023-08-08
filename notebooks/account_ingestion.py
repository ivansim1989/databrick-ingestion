# Databricks notebook source
# MAGIC %run "./utils/data_inegestion_pipeline"

# COMMAND ----------

storage_account = 'dwhmigrationsynapse'

container_name = 'dataverse-collectiusma-org87a06f59'

table_name = 'alternis_account'

output_account = 'qfdynamicshotstorage'

app_id = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-app-id")

service_credential = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-secret")

directory_id = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-dir-id")

processor = DataIngestion(storage_account, container_name, table_name, output_account, app_id, service_credential, directory_id)

processor.main()

# COMMAND ----------

dbutils.notebook.exit("Job Ended")

# COMMAND ----------

t