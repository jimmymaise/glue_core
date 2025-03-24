class Constant:
    TOP_LEVEL_SCOPE = '__main__'
    NULL_AS = 'NULL_STRING__'
    PARQUET_FORMAT = 'parquet'
    TRANS_CTX_DATA_SOURCE_FRAME = 'data_source_frame'
    TRANS_CTX_DROP_NULL_FIELDS = 'drop_null_fields'
    TRANS_CTX_DFC = 'DFC'
    TRANS_CTX_DATA_OUTPUT = 'data_output'
    FULL_LOAD = 'full_load'
    DAILY_LOAD = 'daily_load'
    EMAIL_TEMPLATE_FOLDER = 'email_templates'
    DATE_GENERAL_FORMAT = '%Y-%m-%d'
    DATE_YYYYMMDD_FORMAT = '%Y%m%d'
    DE_IDENTIFY_METHOD_LIST = ['redaction', 'hashing', 'lower_hashing', 'postal_code_masking', 'birth_date_masking',
                               'default']
    STRING_NONE_VALUE = 'None'
    DEFAULT_GLUE_DEPLOY_URL = 's3://{glue_deploy_bucket}/{glue_deploy_s3_folder}/glue_scripts/{etl_id}'
    DEFAULT_MAX_RECORD_PER_PARQUET_FILE = 80000
    DEFAULT_LANDING_DATA_TYPE = 'VARCHAR(2000)'
    DEFAULT_EXTRA_PY_FILES_FOR_PYTHON_SHELL_JOB = [
        f'{DEFAULT_GLUE_DEPLOY_URL}/opus_glue_core-0.1.0-py3-none-any.whl'
    ]

    DEFAULT_EXTRA_PY_FILES_FOR_GLUE_ETL_JOB = [
        f'{DEFAULT_GLUE_DEPLOY_URL}/opus_glue_core.zip',
        f'{DEFAULT_GLUE_DEPLOY_URL}/external_libs/glue-lib.zip'
    ]

    DEFAULT_EXTRA_JARS_FOR_DELTA_LAKE = [
        'delta-core_2.12-1.0.1.jar'
    ]

