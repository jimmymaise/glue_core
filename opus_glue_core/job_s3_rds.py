from pathlib import Path

from pyspark.sql.types import StructType, StringType, StructField

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, from_json
from pyspark.sql.types import StringType

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from core.config import Config

etl_local_path = str(Path(__file__).resolve().parents[0])


class SampleETLGLue:
    def __init__(self):
        self.is_local_env, self.spark, self.glue_context, self.spark_context = Config.get_spark_glue_configs(
            is_delta_lake=True)
        self.dynamodb_table_name, self.etl_id = Config.get_dynamodb_etl_id(etl_local_path)

        self.conf, self.execution_date = Config.get_etl_config(etl_id=self.etl_id,
                                                               table_name=self.dynamodb_table_name,
                                                               is_local=self.is_local_env,
                                                               etl_local_path=etl_local_path)

        self.logger = self.glue_context.get_logger()

        argument_list = self.conf.get('glue_default_args', {})

        glue_job_arguments = Config.get_glue_job_argument(argument_list) if not self.is_local_env \
            else self.conf.get('glue_default_args', {})

    def main(self):

        # Define S3 path
        s3_path = "s3://lead-export-pg/CompanyProfile/exported_data-000000000000.csv"

        # Read CSV files from S3
        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("quote", '"') \
            .option("escape", '"') \
            .load(s3_path)

        # Define JSON schema (array of strings for images)
        json_schema = StringType()  # `images` is JSON, but we store it as a string before converting

        # Convert images column to a proper JSON format
        df = df.withColumn("images", from_json(lit("[]"), json_schema))

        # Convert to Glue DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "dynamic_frame")

        # Write to RDS (PostgreSQL)
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="postgresql",
            connection_options={
                "url": "jdbc:postgresql://app-stg-postgres.c18e626c6haj.us-east-1.rds.amazonaws.com:5432/prod_lid_app_db",
                "user": "app_user",
                "password": "kJ9mP2vL5nX8qR4",
                "dbtable": "public.\"CompanyProfile\"",
                "preactions": "TRUNCATE TABLE public.\"CompanyProfile\";"  # Clears the table before inserting new data
            }
        )

        print("Data successfully written to PostgreSQL")


if __name__ == '__main__':
    SampleETLGLue().main()
