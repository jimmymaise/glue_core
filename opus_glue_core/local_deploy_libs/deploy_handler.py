import argparse
import copy
import datetime
import json
import os
import pathlib
import shutil
import subprocess

from jinja2 import Template

from opus_glue_core.core.common import Common
from opus_glue_core.core.config import Config
from opus_glue_core.core.constant import Constant
from opus_glue_core.core.file.file_handler import FileHandler


class DeployHandler:

    def __init__(self, etl_local_path):
        parser = argparse.ArgumentParser()
        parser.add_argument('-dt', '--dynamodb_table', action='store', dest='dynamodb_table',
                            help='DynamoDB Config Table for Glue etl')
        params = parser.parse_args()
        self.file_handler = FileHandler()
        self.dynamodb_table = params.dynamodb_table
        self.etl_local_path = etl_local_path
        self.root_project_path = str(pathlib.Path(etl_local_path).resolve().parents[1])
        self.root_package_path = str(pathlib.Path(__file__).resolve().parents[1])
        self.core_local_path = f"{self.root_package_path}/core"
        self.local_env_conf = self.get_glue_local_env_config()
        self.glue_etl_config_id = self.local_env_conf['etl_id']
        self.local_const_conf = self.get_glue_local_const_config()
        if not self.dynamodb_table:
            raise Exception('Please provide dynamodb table name for glue etl when running deploy script')

        self.conf, self.execution_date = Config.get_etl_config(etl_id=self.glue_etl_config_id,
                                                               table_name=self.dynamodb_table,
                                                               is_local=True,
                                                               etl_local_path=etl_local_path)

        self.env_conf = Config.get_env_config_from_dynamodb(etl_id=self.glue_etl_config_id,
                                                            table_name=self.dynamodb_table)
        self.deploy_config = copy.deepcopy(self.conf['deploy_config'])
        self.s3_deploy_url = Common.get_s3_deploy_url(self.conf['deploy_config'])
        self.s3_data_url = f"{self.s3_deploy_url}/data"

    def get_glue_local_env_config(self):
        with open(f'{self.etl_local_path}/config/env_config.json', 'r') as read_file:
            dict_data = json.load(read_file)
        return dict_data

    def get_glue_local_const_config(self):
        with open(f'{self.etl_local_path}/config/const_config.json', 'r') as read_file:
            dict_data = json.load(read_file)
        return dict_data

    def make_config_python_file(self):
        with open(f'{self.etl_local_path}/config/const_config.json') as config_json_file:
            data = json.load(config_json_file)
            data = json.dumps(data)

            config_content = f"CONST_CONFIG = '{data}'"

        with open(f'{self.core_local_path}/const_config.py', 'w') as config_python_file:
            config_python_file.write(config_content)

    def make_yaml_file_from_template(self):

        with open(f'{self.root_package_path}/awsglue/glue-job-create-template.sh') as file_content:
            template = Template(file_content.read())

        if self.conf['deploy_config']['glue_job_type'] == 'pythonshell':
            default_extra_py_files = Constant.DEFAULT_EXTRA_PY_FILES_FOR_PYTHON_SHELL_JOB
        else:
            default_extra_py_files = Constant.DEFAULT_EXTRA_PY_FILES_FOR_GLUE_ETL_JOB

        extra_py_files = (','.join(default_extra_py_files)).format(
            **self.deploy_config
        ).replace('//glue_scripts', '/glue_scripts')

        self.deploy_config['default_arguments'] = {
            '--extra-py-files': extra_py_files,
            '--job-bookmark-option': f"{self.deploy_config['job_bookmark_option']}",
            "--glue_dynamodb_table": f"{self.deploy_config['glue_dynamodb_table']}",
            "--etl_id": f"{self.deploy_config['etl_id']}"
        }
        external_jars = self.conf['deploy_config'].get('extra_jars', [])

        if self.conf['deploy_config'].get('is_delta_lake'):
            external_jars += Constant.DEFAULT_EXTRA_JARS_FOR_DELTA_LAKE
            self.deploy_config['default_arguments']['--additional-python-modules'] = 'delta-spark==1.0.1'

        extra_jars_str = self._upload_external_jar_file_for_glue_job(
            external_jars=external_jars)

        self.deploy_config['default_arguments']['--extra-jars'] = extra_jars_str
        for key, value in self.conf['glue_default_args'].items():
            self.deploy_config['default_arguments'][f'--{key}'] = value

        env = self.conf.get('env')
        if env:
            self.deploy_config['glue_job_name'] = f"{env}_{self.deploy_config['glue_job_name']}"

        self.deploy_config['default_arguments'] = json.dumps(self.deploy_config['default_arguments'])
        self.deploy_config['s3_deploy_url'] = self.s3_deploy_url
        output = template.render(**self.deploy_config)
        # to save the results
        output_file_path = f"{self.deploy_config['local_temp_folder_path']}" \
                           f"/output_{self.deploy_config['etl_id']}.sh"
        with open(output_file_path, "w") as yaml_deploy_file:
            yaml_deploy_file.write(output)
        return output_file_path

    def _upload_external_jar_file_for_glue_job(self, external_jars):
        extra_jar_files_str = ''
        for file_name in external_jars:
            subprocess.run(
                ["aws", "s3", "cp",
                 f"{self.root_package_path}/glue_external_jars/{file_name}",
                 f"{self.s3_deploy_url}/external_jars/{file_name}", ])
            extra_jar_files_str += f',{self.s3_deploy_url}/external_jars/{file_name}'
            extra_jar_files_str = extra_jar_files_str.strip(',')
        return extra_jar_files_str

    def _build_lib_and_upload_for_python_shell(self):
        timestamp = datetime.datetime.now().timestamp()
        build_path = f'/tmp/{timestamp}/build'
        self.file_handler.copy_tree(self.root_package_path, f"{build_path}/opus_glue_core",
                                    ignore_paths=[f"{self.root_package_path}/awsglue",
                                                  f"{self.root_package_path}/etl_test",
                                                  f"{self.root_package_path}/external_libs",
                                                  f"{self.root_package_path}/glue_external_jars",
                                                  f"{self.root_package_path}/local_deploy_libs",
                                                  f"{self.root_package_path}/local_run_jars",
                                                  f"{self.core_local_path}/spark_udf"
                                                  ])
        self.file_handler.copy_file(f"{self.root_package_path}/python_shell_pyproject.toml",
                                    f"{build_path}/pyproject.toml")
        print(f"{build_path}/pyproject.toml")
        wd = os.getcwd()
        os.chdir(f"{build_path}")
        subprocess.run(["poetry", "build"])
        os.chdir(wd)
        whl_path = f'{build_path}/dist/opus_glue_core-0.1.0-py3-none-any.whl'
        subprocess.run(
            ["aws", "s3", "cp",
             whl_path,
             f"{self.s3_deploy_url}/opus_glue_core-0.1.0-py3-none-any.whl", ])

    def _build_and_upload_lib_for_glue_etl_job(self):
        timestamp = datetime.datetime.now().timestamp()
        build_path = f'/tmp/{timestamp}/build/opus_glue_core'
        self.file_handler.copy_tree(self.root_package_path, build_path,
                                    ignore_paths=[f"{self.root_package_path}/awsglue",
                                                  f"{self.root_package_path}/etl_test",
                                                  f"{self.root_package_path}/external_libs",
                                                  f"{self.root_package_path}/glue_external_jars",
                                                  f"{self.root_package_path}/local_deploy_libs",
                                                  f"{self.root_package_path}/local_run_jars",
                                                  ])

        shutil.make_archive(build_path, 'zip', f'{build_path}/..', 'opus_glue_core')
        shutil.make_archive(f'{self.root_package_path}/external_libs/glue-lib', 'zip',
                            f'{self.root_package_path}/external_libs/',
                            'glue-lib')

        subprocess.run(
            ["aws", "s3", "cp",
             f"{build_path}/../opus_glue_core.zip",
             f"{self.s3_deploy_url}/opus_glue_core.zip", ])
        subprocess.run(
            ["aws", "s3", "cp",
             f"{self.root_package_path}/external_libs/glue-lib.zip",
             f"{self.s3_deploy_url}/external_libs/glue-lib.zip", ])

    def deploy_glue(self):
        result = Common.get_missing_key_between_two_dicts(dict_1=self.local_env_conf, dict_2=self.env_conf)
        if result:
            raise ValueError(
                f'Mismatch in parent keys or their sub-keys between dynamo config and local env_config file! \n'
                f'ETL id: {self.env_conf["etl_id"]}\n'
                f'List parent keys: {result}. Check dynamo config and local config file again!\n'
                f'Please notice that we should update dynamo config based on config file'
                f'before running deployment')
        template_file_path = self.make_yaml_file_from_template()

        self.make_config_python_file()

        subprocess.run(
            ["aws", "s3", "cp",
             "glue-job.py",
             f"{self.s3_deploy_url}/glue-job.py", ])

        if self.conf['deploy_config']['glue_job_type'] == 'pythonshell':
            self._build_lib_and_upload_for_python_shell()
        else:
            self._build_and_upload_lib_for_glue_etl_job()

        if pathlib.Path(f"{self.etl_local_path}/data").is_dir():
            subprocess.run(
                ["aws", "s3", "sync", "--delete",
                 f"{self.etl_local_path}/data",
                 f"{self.s3_data_url}", ])
        else:
            print('Skip sync data to s3 as data folder does not exist')
        subprocess.run(
            ["aws", "s3", "cp",
             f"{self.etl_local_path}/config/const_config.json",
             f"{self.s3_deploy_url}/const_config.json", ])

        subprocess.run(
            ["sh", template_file_path])

        print(
            'Deploy complete! If deploy failed please run below command to '
            'delete job, wait for 2 minutes before continue\n'
            f'Command: aws glue delete-job --job-name {self.deploy_config["glue_job_name"]}')
