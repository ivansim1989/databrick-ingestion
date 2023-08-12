# Databricks notebook source
dbutils.widgets.text("p_storage_account", "")
storage_account = dbutils.widgets.get("p_storage_account")

# COMMAND ----------

dbutils.widgets.text("p_container_name", "")
container_name = dbutils.widgets.get("p_container_name")

# COMMAND ----------

dbutils.widgets.text("p_output_account", "")
output_account = dbutils.widgets.get("p_output_account")

# COMMAND ----------

# MAGIC %run "./utils/data_inegestion_pipeline.py"

# COMMAND ----------

# storage_account = 'dwhmigrationsynapse'

# container_name = 'dataverse-collectiusma-org87a06f59'

# # table_name = 'alternis_account'

# output_account = 'qfdynamicshotstorage'

app_id = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-app-id")

service_credential = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-secret")

directory_id = dbutils.secrets.get(scope="key-vault-secret",key="qf-lakehouse-dir-id")

for item in dbutils.fs.ls(f"mnt/{storage_account}/{container_name}/"):
    if item.path.endswith("/") and not item.path.endswith("Microsoft.Athena.TrickleFeedService/") and not item.path.endswith("OptionsetMetadata/"):
        table_name = item.path.split('/')[4]
        processor = DataIngestion(storage_account, container_name, table_name, output_account, app_id, service_credential, directory_id)
        processor.main()

# COMMAND ----------

dbutils.notebook.exit("Job Ended")