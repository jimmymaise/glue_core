from opus_glue_core.core.file.s3_handler import S3Handler


class DMSHandler:
    def __init__(self, dms_target_url):
        self.dms_target_url = dms_target_url
        self.s3_handler = S3Handler()
        self.target_bucket_name, dms_target_path = \
            self.s3_handler.get_bucket_name_and_file_path_from_s3_url(dms_target_url)

    def get_dms_table_folder_urls(self):
        table_urls = []
        schema_urls = self.s3_handler.list_top_level_objects_from_s3_url(self.dms_target_url)
        if schema_urls is None:
            raise Exception(
                f'{self.dms_target_url} is empty.  Confirm the source DB has data and the DMS replication task '
                f'is replicating to this S3 location. '
                f'Review the documentation https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.html '
                f'to ensure the DB is setup for CDC.')
        for schema_url in schema_urls:
            table_urls += self.s3_handler.list_top_level_objects_from_s3_url(s3_url=schema_url)
        return table_urls
