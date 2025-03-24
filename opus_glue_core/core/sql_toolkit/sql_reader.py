import importlib
import re
from pathlib import Path

from jinja2 import Template

from opus_glue_core.core.common import Dict2Obj


class SQLReader:
    def __init__(self):
        core_path = str(Path(__file__).resolve().parents[1])
        self.query_dir = f'{core_path}/sql_templates'
        self.query_name_definition_pattern = re.compile(r"--\s*SQL_BLOCK_CODE\s*:\s*")
        self.empty_pattern = re.compile(r"^\s*$")
        self.doc_comment_pattern = re.compile(r"\s*--\s*(.*)$")
        self.var_pattern = re.compile(r"\$.+\$")

    @staticmethod
    def add_template_filter_functions(filter_funcs):
        env = Template("").environment
        if not isinstance(filter_funcs, list):
            filter_funcs = [filter_funcs]
        for filter_func in filter_funcs:
            env.filters[filter_func.__name__] = filter_func

    def _get_query_data(self, query_str):
        sql = ""
        lines = query_str.strip().splitlines()
        query_name = lines[0].replace("-", "_")
        for line in lines[1:]:
            doc_match = self.doc_comment_pattern.match(line)
            if not doc_match:
                sql += line + "\n"
        return query_name, sql

    def get_queries(self, query_file_path):
        sql_template = importlib.import_module(f'core.sql_templates.{query_file_path}', 'TEMPLATE').TEMPLATE
        query_string_dict = {}
        for query_sql_str in self.query_name_definition_pattern.split(sql_template):
            if not self.empty_pattern.match(query_sql_str):
                query_name, sql = self._get_query_data(query_sql_str)
                var_match = self.var_pattern.findall(sql)
                for var in var_match:
                    key = var.replace('$', '')
                    if key not in query_string_dict:
                        continue
                    sql = sql.replace(var, query_string_dict[key].rstrip('\n').rstrip(';'))
                query_string_dict[query_name] = sql
        query_template = {key: Template(value).render for
                          key, value in query_string_dict.items()}
        query_template['__sql_file__path__'] = query_file_path
        return Dict2Obj(query_template)

    @staticmethod
    def get_query_from_string(query_string, **params):
        return Template(query_string).render(**params)
