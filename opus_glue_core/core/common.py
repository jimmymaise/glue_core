import base64
import datetime
import decimal
import json
import math
from collections.abc import MutableMapping
from datetime import timedelta
from opus_glue_core.core.constant import Constant


class Common:

    @staticmethod
    def get_core_path():
        from opus_glue_core import core
        path = os.path.dirname(core.__file__)

    @staticmethod
    def rec_merge(d1, d2):
        """
        Update two dicts of dicts recursively,
        if either mapping has leaves that are non-dicts,
        the second's leaf overwrites the first's.
        :param d1:
        :param d2:
        :return:
        """
        for k, v in d1.items():  # in Python 2, use .iteritems()!
            if k in d2 and all(isinstance(e, MutableMapping) for e in (v, d2[k])):
                d2[k] = Common.rec_merge(v, d2[k])
            # we could further check types and merge as appropriate here.
        d3 = d1.copy()
        d3.update(d2)
        return d3

    @staticmethod
    def get_dict_shape(dict_):
        if isinstance(dict_, dict):
            return {k: Common.get_dict_shape(dict_[k]) for k in dict_}
        else:
            # Replace all non-dict values with None.
            return None

    @staticmethod
    def get_missing_key_between_two_dicts(dict_1, dict_2):
        dict_1_shape = Common.get_dict_shape(dict_1)
        dict_2_shape = Common.get_dict_shape(dict_2)
        all_keys = set(list(dict_1_shape.keys()) + list(dict_2_shape.keys()))

        parent_missing_keys = [key for key in all_keys if not (key in dict_1_shape and key in dict_2) or
                               dict_1_shape[key] != dict_2_shape[key]]

        for index, parent_key in enumerate(parent_missing_keys):
            if dict_1_shape.get(parent_key) and dict_2_shape.get(parent_key):
                parent_missing_keys[index] = \
                    f'{parent_key}-> ' \
                    f'{Common.get_missing_key_between_two_dicts(dict_1_shape[parent_key], dict_2_shape[parent_key])}'

        return parent_missing_keys

    @staticmethod
    def merge_dict_lists(list1, list2, merge_key):
        merged = {}
        for item in list1 + list2:
            if item[merge_key] in merged:
                merged[item[merge_key]].update(item)
            else:
                merged[item[merge_key]] = item
        return [val for (_, val) in merged.items()]

    @staticmethod
    def change_time_string_format(time_string, current_format, change_format):

        date_time_obj = datetime.datetime.strptime(time_string, current_format)
        return date_time_obj.strftime(change_format)

    @staticmethod
    def replace_none_value(value_check, value_replace_when_none):
        return value_check if value_check is not None else value_replace_when_none

    @staticmethod
    def remove_sensitive_info_from_text(text, sensitive_info_list, replace_text):
        for sensitive_info in sensitive_info_list:
            text = text.replace(sensitive_info, replace_text)
        return text

    @staticmethod
    def encode_base_64(text):
        return str(base64.b64encode(bytes(text, "utf-8")), "utf-8")

    @staticmethod
    def decode_base_64(text):
        return str(base64.b64decode(bytes(text, "utf-8")), "utf-8")

    @staticmethod
    def convert_snake_case_to_camel_case(word):
        return ''.join(x.capitalize() or '_' for x in word.split('_'))

    @staticmethod
    def convert_snake_case_to_pascal_case(word):
        return ''.join(x.capitalize() or '_' for x in word.split('_'))

    @staticmethod
    def get_s3_deploy_url(deploy_config):
        return Constant.DEFAULT_GLUE_DEPLOY_URL.format(**deploy_config).replace('//glue_scripts', '/glue_scripts')

    @staticmethod
    def remove_duplicate_item_in_list_dict(list_dict, key_check_list, raise_if_mismatch=False):
        unique = {}
        unique_obj_list = []

        for item in list_dict:
            str_check = ""

            for key in key_check_list:
                if not item.get(key):
                    raise ValueError(
                        '{item} does not have key {key} which exists in key_check_list'.format(item=item, key=key))
                str_check += str(item[key])

            if str_check not in unique:
                unique[str_check] = item
            else:
                if raise_if_mismatch and unique[str_check] != item:
                    raise Exception(
                        'Duplicated item  {item} with same value with keys in key_check_list but not others'.format(
                            item=item))

        return unique_obj_list

    @staticmethod
    def execution_date_add(execution_date, days):
        return execution_date + timedelta(days)

    @staticmethod
    def is_date_format(date_text, date_format):
        try:
            datetime.datetime.strptime(date_text, date_format)
            return True
        except ValueError:
            return False

    @staticmethod
    def update_column_list(column_list):
        for index, item in enumerate(column_list):
            if 'group' == item.lower():
                column_list[index] = '"group"'
        return column_list

    @staticmethod
    def add_columns_to_data_frame(columns, dataframe, default_value=None):
        import pyspark.sql.functions as sf
        from pyspark.sql.types import StringType
        missing_columns = set(columns) - set(dataframe.columns)
        for column in missing_columns:
            dataframe = dataframe.withColumn(column, sf.lit(default_value).cast(StringType()))
        return dataframe


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class Page(object):

    def __init__(self, cal_page_data_func, page_size, total_record):
        if page_size <= 0:
            raise AttributeError('page_size needs to be >= 1')
        self.current_page = 1
        self.current_offset = 0

        self.total_page = math.ceil(total_record / page_size)
        self.page_size = page_size
        self.cal_page_data_func = cal_page_data_func
        self.total_record = total_record

    def __iter__(self):
        return self

    def __next__(self):
        self.valid_current_page = self.current_page <= self.total_page
        if not self.valid_current_page:
            raise StopIteration
        data, max_increase_column_value = self.cal_page_data_func(page_size=self.page_size, offset=self.current_offset)
        self.current_offset = max_increase_column_value
        self.current_page += 1
        return data


class Dict2Obj:
    def __init__(self, dictionary):
        for key in dictionary:
            setattr(self, key, dictionary[key])

    def __repr__(self):
        """"""
        return "<Dict2Obj: %s>" % self.__dict__

    def __getitem__(self, key):
        return self.__dict__[key]
