class FileHandler:

    @staticmethod
    def get_data_frame_from_files(spark, path, extension, is_folder=True, base_path=None,
                                  ignore_not_exist_exception=False, **csv_options):
        df = None
        path_s3a = path.replace('s3://', 's3a://')
        file_path = f'{path_s3a}*.{extension}' if is_folder is False else f'{path_s3a}*'
        spark_read = spark.read
        if base_path:
            spark_read.option('basePath', base_path)

        if extension == 'csv.gz':
            extension = 'csv'

        if extension == 'csv':
            spark_read = spark_read \
                .option('header', str(csv_options.get('header', 'false').lower())) \
                .option('delimiter', csv_options.get('delimiter', ','))
        try:
            df = spark_read \
                .format(extension) \
                .load(file_path)
        except Exception as e:
            if 'Path does not exist' in str(e) and ignore_not_exist_exception:
                print(f'Path does not exist! {e}')
                return None
            raise e
        return df

    @staticmethod
    def get_data_frame_from_parquet_path_list(spark, s3_file_list):
        s3a_paths = [sub.replace('s3://', 's3a://') for sub in s3_file_list]
        spark_read = spark.read

        df = spark_read. \
            format("parquet") \
            .load(s3a_paths)
        return df

    @staticmethod
    def divide_chunks(search_file_list, batch_size):
        for i in range(0, len(search_file_list), batch_size):
            yield search_file_list[i:i + batch_size]
