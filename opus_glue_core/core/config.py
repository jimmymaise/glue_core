import importlib
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from boto3 import Session

from opus_glue_core.core.common import Common
from opus_glue_core.core.common import DecimalEncoder

resolved_options = None
local_run_jars_path = f'{str(Path(__file__).resolve().parents[1])}/local_run_jars'
external_jars_path = f'{str(Path(__file__).resolve().parents[1])}/glue_external_jars'

logger = None


def get_logger():
    return logger


class Config:
    @classmethod
    def is_local_env(cls):
        return os.path.exists(local_run_jars_path)

    @classmethod
    def get_local_config(cls, etl_local_path):
        with open(f'{etl_local_path}/config/config.json', 'r') as read_file:
            local_conf = json.load(read_file)
        return local_conf

    @classmethod
    def get_dynamodb_etl_id(cls, etl_local_path):
        if not cls.is_local_env():
            args = cls.get_glue_job_argument(argument_list=['glue_dynamodb_table', 'etl_id'])
            glue_dynamodb_table = args['glue_dynamodb_table']
            etl_id = args['etl_id']

        else:
            with open(f'{etl_local_path}/config/const_config.json', 'r') as read_file:
                local_conf = json.load(read_file)
            glue_dynamodb_table = local_conf.get('glue_default_args', {}).get('glue_dynamodb_table', {})
            etl_id = local_conf['etl_id']

        return glue_dynamodb_table, etl_id

    @classmethod
    def get_glue_job_argument(cls, argument_list):
        if not cls.is_local_env():
            glue_utils = importlib.import_module("awsglue.utils")
            args = glue_utils.getResolvedOptions(sys.argv, argument_list)

        else:
            args = {}
        return args

    @classmethod
    def get_etl_config(cls, etl_id, table_name, is_local, etl_local_path=None):

        env_conf = {}

        if is_local:
            with open(f'{etl_local_path}/config/const_config.json', 'r') as read_file:
                const_conf = json.load(read_file)
            if const_conf['is_use_env_config_file_for_debug']:
                with open(f'{etl_local_path}/config/env_config.json', 'r') as read_file:
                    env_conf = json.load(read_file)
            data_folder_path = "/data/local"
        else:
            mod = importlib.import_module("opus_glue_core.core.const_config")
            const_conf = json.loads(getattr(mod, 'CONST_CONFIG'))
            data_folder_path = "/data"
        if not env_conf:
            env_conf = cls.get_env_config_from_dynamodb(table_name, etl_id)

        config = Common.rec_merge(const_conf, env_conf)
        config['deploy_config'].update({
            'etl_id': etl_id,
            'glue_dynamodb_table': table_name
        })
        if Path(f"{etl_local_path}/data").is_dir():
            cls.sync_etl_data_folder_to_s3(config, etl_local_path, data_folder_path)
        execution_date = cls.get_execution_date_from_config(config)
        return config, execution_date

    @classmethod
    def get_env_config_from_dynamodb(cls, table_name, etl_id):
        session = Session(region_name='us-west-2')
        dynamo_resource = session.resource("dynamodb")

        table = dynamo_resource.Table(table_name)
        key = {'etl_id': etl_id}
        response = table.get_item(Key=key)
        return json.loads(json.dumps(response['Item'], cls=DecimalEncoder))

    @classmethod
    def get_execution_date_from_config(cls, config):
        if config.get('execution_date_for_debug'):
            return config['execution_date_for_debug']
        time_delta_for_execution_date = config.get('time_delta_for_execution_date') or 0
        return datetime.strftime(datetime.now() - timedelta(time_delta_for_execution_date), '%Y-%m-%d')

    @classmethod
    def get_spark_glue_configs(cls, *, is_delta_lake, spark_config: dict = None):
        global logger
        from pyspark.sql import SparkSession

        is_local_env = cls.is_local_env()
        glue_context_mod = importlib.import_module("opus_glue_core.awsglue.context") if is_local_env \
            else importlib.import_module("awsglue.context")

        spark_config = spark_config or {}

        if not is_local_env:
            if is_delta_lake:
                spark_config["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
                spark_config["spark.sql.catalog.spark_catalog"] = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

            spark_builder = SparkSession.builder
            for key, value in spark_config.items():
                spark_builder = spark_builder.config(key, value)

            spark = spark_builder.getOrCreate()
            spark_context = spark.sparkContext
            glue_context = glue_context_mod.GlueContext(spark_context)

        else:
            console = logging.StreamHandler(sys.stdout)

            logger = logging.getLogger("LOCAL_LOG")
            logger.addHandler(console)
            logger.setLevel(logging.INFO)
            logger.warning("Running on local env. Should have java 1.8.x (8.x). Your current java version is:")
            subprocess.run(
                ["java", "-version"])
            logger.warning("Your AWS identity is:")
            subprocess.run(
                ["aws", "sts", "get-caller-identity"])
            cls._fix_aws_sso_issue()
            jar_local_paths = [os.path.join(local_run_jars_path, x) for x in os.listdir(local_run_jars_path)]
            jar_external_paths = [os.path.join(external_jars_path, x) for x in os.listdir(external_jars_path)]
            jar_paths = jar_local_paths + jar_external_paths
            spark = SparkSession \
                .builder \
                .appName("Glue-Local") \
                .config("spark.jars", ",".join(jar_paths)) \
                .config("spark.local.dir", "/tmp") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()

            spark_context = spark.sparkContext
            glue_context = glue_context_mod.GlueContext(spark_context)

            glue_context.get_logger = lambda: logger
        return is_local_env, spark, glue_context, spark_context

    @classmethod
    def get_python_shell_configs(cls):
        global logger

        is_local_env = cls.is_local_env()
        if not is_local_env:
            logger = logging.getLogger()
        else:
            cls._fix_aws_sso_issue()
            console = logging.StreamHandler(sys.stdout)
            logger = logging.getLogger("LOCAL_LOG")
            logger.addHandler(console)
            logger.setLevel(logging.INFO)

        return is_local_env, logger

    @staticmethod
    def _fix_aws_sso_issue():
        session = boto3.Session()
        cred = session.get_credentials()
        os.environ['AWS_ACCESS_KEY_ID'] = cred.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = cred.secret_key
        if cred.token:
            os.environ['AWS_SESSION_TOKEN'] = cred.token
        if session.region_name:
            os.environ['AWS_DEFAULT_REGION'] = session.region_name

    @staticmethod
    def sync_etl_data_folder_to_s3(config, etl_local_path, data_folder_path):
        print('Sync etl data folder to s3')
        s3_deploy_url = Common.get_s3_deploy_url(deploy_config=config['deploy_config'])
        s3_data_url = f"{s3_deploy_url}/{data_folder_path}"
        subprocess.run(
            ["aws", "s3", "sync", "--delete",
             f"{etl_local_path}/data",
             f"{s3_data_url}", ])
        config['s3_data_url'] = s3_data_url
