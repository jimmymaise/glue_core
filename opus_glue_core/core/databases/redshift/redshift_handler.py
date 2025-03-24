import copy


class RedShiftHandler:

    def __init__(self, db_name, db_user, db_pass, host_ip, aws_iam_role, **kwargs):

        self.glue_context = kwargs['glue_context']
        self.redshift_tmp_dir = kwargs['redshift_tmp_dir']
        self.is_local_env = kwargs['is_local_env']
        self.catalog_connection = kwargs.get('catalog_connection')
        self.connection_options = {
            "url": f"jdbc:redshift://{host_ip}:{kwargs.get('port', 5439)}/{db_name}",
            "user": db_user,
            "password": db_pass,
            "redshiftTmpDir": self.redshift_tmp_dir,
            "aws_iam_role": aws_iam_role,
            "database": db_name
        }

    def get_dynamic_frame_from_redshift_table(self, table_name_with_schema):
        self.connection_options["dbtable"] = table_name_with_schema

        return self.glue_context.create_dynamic_frame_from_options("redshift", self.connection_options)

    def write_dynamic_frame_to_redshift_table(self, dynamic_frame, table_name_with_schema, pre_actions=None,
                                              post_actions=None,
                                              catalog_connection=None,
                                              extra_copy_options_str=None):
        catalog_connection = catalog_connection or self.catalog_connection

        self.connection_options["dbtable"] = table_name_with_schema
        connection_options = copy.deepcopy(self.connection_options)

        if pre_actions:
            connection_options['preactions'] = pre_actions

        if post_actions:
            connection_options['postactions'] = post_actions

        if extra_copy_options_str:
            connection_options['extracopyoptions'] = extra_copy_options_str

        if catalog_connection and not self.is_local_env:
            self.glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                                 catalog_connection=catalog_connection,
                                                                 redshift_tmp_dir=self.redshift_tmp_dir,
                                                                 connection_options=connection_options)

        else:
            self.glue_context.write_dynamic_frame.from_options(frame=dynamic_frame,
                                                               connection_type='redshift',
                                                               connection_options=connection_options)
