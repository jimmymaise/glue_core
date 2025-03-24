from pathlib import Path

from pyspark.sql.types import StructType, StringType, StructField

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
        self.logger.info(self.conf['other_params'])

        schema = StructType([
            StructField("name", StringType(), False),
            StructField("birth_date", StringType(), False),
            StructField("postal", StringType(), False)
        ])

        sample_data = [('Alice', '1994-10-10 10:10:20', '77299999')]
        s3_file_path = f"s3://{self.conf['deploy_config']['glue_deploy_bucket']}/temp/test_glue/"

        origin_df = self.spark.createDataFrame(sample_data, schema)
        origin_df.write.format("parquet").mode("overwrite").save(s3_file_path)
        output_gdf = None
        input_gdf = self.glue_context.create_dynamic_frame_from_options(connection_type="s3", connection_options={
            "paths": [
                s3_file_path]},
                                                                        format="parquet")
        output_gdf = self.glue_context.write_dynamic_frame. \
            from_options(frame=input_gdf, connection_type="s3",
                         connection_options={
                             "path": f"s3://{self.conf['deploy_config']['glue_deploy_bucket']}/temp/test_glue/"},
                         format="parquet")
        self.logger.info(f'Sample glue success')
        print('OK')
        return output_gdf


if __name__ == '__main__':
    SampleETLGLue().main()
