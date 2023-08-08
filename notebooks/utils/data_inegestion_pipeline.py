# Databricks notebook source
import logging
import json
from pyspark.sql.functions import udf, col, input_file_name, lit, current_timestamp, from_utc_timestamp, date_format, substring
from pyspark.sql.types import StringType, IntegerType, BooleanType, DecimalType, DoubleType, StructType, StructField
import pytz
from datetime import datetime

# COMMAND ----------

class DataIngestion:
    def __init__(self, storage_account, container_name, table_name, output_account, app_id, service_credential, directory_id):
        """
        Initialize the DataIngestion class.
        Args:
            storage_account (str): The storage account name.
            container_name (str): The container name.
            table_name (str): The table name.
            output_account (str): The output storage account name.
            app_id (secret): The application id.
            service_credential (secret): The service credential.
            directory_id (secret): The directory id.
        """
        self.logger = self.setup_logging()
        self.storage_account = storage_account
        self.container_name = container_name
        self.table_name = table_name
        self.output_account = output_account
        self.app_id = app_id
        self.service_credential = service_credential
        self.directory_id = directory_id


    def setup_logging(self, level=logging.DEBUG):
        """
        Set up logging with the specified log level.
        Args:
            level (int): The log level to set.
        Returns:
            logging.Logger: The logger instance.
        """
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
        )

        logger = logging.getLogger(__name__)
        logger.setLevel(level)

        return logger

    def create_mount(self, container_name, storage_account):
        """
        """
        try:
            # Check if the mount point is mounted
            dbutils.fs.ls(f"/mnt/{self.storage_account}/{self.container_name}")
        except:
            configs = {"fs.azure.account.auth.type": "OAuth",
                            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                            "fs.azure.account.oauth2.client.id": self.app_id,
                            "fs.azure.account.oauth2.client.secret": self.service_credential,
                            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{self.directory_id}/oauth2/token"
                        }
            dbutils.fs.mount(
                            source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
                            mount_point = f"/mnt/{storage_account}/{container_name}",
                            extra_configs = configs
                        )

    def get_latest_files(self):
        """
        Get a list of latest file paths from the specified snapshot folder.
        Returns:
            list: A list of latest file paths.
        """
        self.create_mount(self.container_name, self.storage_account)
        snapshot_path = f"/mnt/{self.storage_account}/{self.container_name}/{self.table_name}/Snapshot"
        self.logger.info("Read snaphot folder: %s", snapshot_path)
        snapshot = dbutils.fs.ls(snapshot_path)
        file_list = [item.path for item in snapshot]
        latest_files_dict = {}

        for file_path in file_list:
            year_month = file_path.split("/")[-1].split("_")[0]
            timestamp = int(file_path.split('_')[2].split('.')[0])
            if year_month not in latest_files_dict or timestamp > latest_files_dict[year_month][0]:
                latest_files_dict[year_month] = (timestamp, file_path)

        latest_files = [file_path for _, file_path in latest_files_dict.values()]
        self.logger.info("Latest files: %s", latest_files)
        return latest_files

    def get_additional_data(self):
        """
        Read additional data from the metadata file.
        Returns:
            dict: A dictionary containing additional data.
        """
        metadata_path = f"/dbfs/mnt/{self.storage_account}/{self.container_name}/Microsoft.Athena.TrickleFeedService/{self.table_name}-EntityMetadata.json"
        self.logger.info("Read metadata file: %s", metadata_path)
        with open(metadata_path, "r") as json_file:
            additional_data = json.load(json_file)
        return additional_data

    def get_schema(self):
        """
        Get the schema for the data.
        Returns:
            StructType: The schema for the data as a StructType object.
        """
        model_path = f"/dbfs/mnt/{self.storage_account}/{self.container_name}/Microsoft.Athena.TrickleFeedService/{self.table_name}-model.json"
        self.logger.info("Read model file: %s", model_path)

        with open(model_path, "r") as model_json:
            model_data = json.load(model_json)

        schema_list = model_data["entities"][0]["attributes"]
        transformed_fields = []

        data_type_mapping = {
            'guid': StringType(),
            'string': StringType(),
            'dateTime': StringType(),
            'int64': IntegerType(),
            'boolean': BooleanType(),
            'decimal': DecimalType(),
            'dateTimeOffset': StringType(),
            'double': DoubleType()
        }
        for field_dict in schema_list:
            field_name = field_dict['name']
            data_type_str = field_dict['dataType']

            spark_type = data_type_mapping.get(data_type_str)

            if data_type_str == 'decimal':
                decimals = field_dict.get('cdm:traits', [{}])[0].get('arguments', [])
                precision = decimals[0]['value']
                scale = decimals[1]['value']
                spark_type = DecimalType(precision, scale)

            nullable = False if field_name.lower() == 'id' else True

            transformed_fields.append(StructField(field_name, spark_type, nullable))

        schema = StructType(fields=transformed_fields)
        self.logger.info("Schema output: %s", schema)
        return schema

    def option_set_metadata(self, df, data_dict):
        """
        Add metadata related to option sets to the DataFrame.
        Args:
            df (DataFrame): The DataFrame to which metadata will be added.
            data_dict (dict): The metadata dictionary.
        Returns:
            DataFrame: The DataFrame with added option set metadata.
        """
        broadcast_variables = {
            f"b_{option_set_name}": spark.sparkContext.broadcast(
                {item["Option"]: item["LocalizedLabel"] for item in data_dict if item["OptionSetName"] == option_set_name}
            )
            for option_set_name in set(item["OptionSetName"] for item in data_dict)
        }

        for broadcast_name in broadcast_variables:
            option_set_name = broadcast_name[2:]
            udf_function = udf(
                lambda value: broadcast_variables[broadcast_name].value.get(value),
                StringType()
            )
            df = df.withColumn(f"{option_set_name}name", udf_function(col(option_set_name)))
            self.logger.info("Field created: %s", f"{option_set_name}name")
        return df

    def status_metadata(self, df, data_dict):
        """
        Add metadata related to status codes to the DataFrame.
        Args:
            df (DataFrame): The DataFrame to which metadata will be added.
            data_dict (dict): The metadata dictionary.
        Returns:
            DataFrame: The DataFrame with added status code metadata.
        """
        status_metadata = {item["Status"]: item["LocalizedLabel"] for item in data_dict}
        b_status_metadata = spark.sparkContext.broadcast(status_metadata)
        status_metadata_udf = udf(
            lambda value: b_status_metadata.value.get(value),
            StringType()
        )
        df = df.withColumn("statuscodename", status_metadata_udf(col("statuscode")))
        self.logger.info("Field created: statuscodename")
        return df

    def state_metadata(self, df, data_dict):
        """
        Add metadata related to state codes to the DataFrame.
        Args:
            df (DataFrame): The DataFrame to which metadata will be added.
            data_dict (dict): The metadata dictionary.
        Returns:
            DataFrame: The DataFrame with added state code metadata.
        """
        state_metadata = {item["State"]: item["LocalizedLabel"] for item in data_dict}
        b_state_metadata = spark.sparkContext.broadcast(state_metadata)
        state_metadata_udf = udf(
            lambda value: b_state_metadata.value.get(value),
            StringType()
        )
        df = df.withColumn("statecodename", state_metadata_udf(col("statecode")))
        self.logger.info("Field created: statecodename")
        return df

    def audit_log_and_partition(self, df):
        """
        Add audit log information and create a partition based on created date.
        Args:
            df (DataFrame): The DataFrame to which audit log information will be added.
        Returns:
            DataFrame: The DataFrame with added audit log information and partition column.
        """
        local_timestamp = date_format(from_utc_timestamp(current_timestamp(), "Asia/Singapore"), "yyyy-MM-dd HH:mm:ss.SSS")
        current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        df = df.withColumn("file_name", input_file_name()) \
                .withColumn("ingestion_timestamp", local_timestamp) \
                .withColumn("ingestion_notebook", lit(current_notebook_path)) \
                .withColumn("created_date", substring(col("createdon"), 1, 7))
        self.logger.info("Audit logs created")
        return df

    def write_parquet(self, df):
        """
        Write the DataFrame to Parquet format.
        Args:
            df (DataFrame): The DataFrame to be written.
        """

        sg_timezone = pytz.timezone('Asia/Singapore')
        current_datetime = datetime.now(sg_timezone)
        current_date_partition = current_datetime.strftime('%Y%m%d')

        output_container = {
            "dataverse-collectiusma-org87a06f59" : "dynamics-my-prod",
        } 
        self.create_mount(self.output_account, output_container[self.container_name])
        output_path = f"/mnt/{self.output_account}/{output_container[self.container_name]}/{self.table_name}/{current_date_partition}/"

        df.write.partitionBy("created_date").parquet(
            output_path,
            mode="overwrite",
            compression="snappy"
        )
        self.logger.info("Write data into path %s", output_path)

    def main(self):
        """
        Main function to execute the data ingestion pipeline.
        """
        latest_files = self.get_latest_files()
        schema = self.get_schema()
        additional_data = self.get_additional_data()
        df = spark.read.option("delimiter", ",")\
                    .option("multiline","true")\
                    .option("escape", '"' )\
                .csv(latest_files, header=False, schema=schema)
        df = self.option_set_metadata(df, additional_data["GlobalOptionSetMetadata"])
        df = self.option_set_metadata(df, additional_data["OptionSetMetadata"])
        df = self.status_metadata(df, additional_data["StatusMetadata"])
        df = self.state_metadata(df, additional_data["StateMetadata"])
        df = self.audit_log_and_partition(df)
        self.write_parquet(df)
        self.logger.info("Job Done")


