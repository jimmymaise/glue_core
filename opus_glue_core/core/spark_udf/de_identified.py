import hashlib
from datetime import date, datetime
from functools import wraps

import dateutil.parser as parser
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

from opus_glue_core.core.common import Common
from opus_glue_core.core.constant import Constant


def udf_func(output_type=StringType()):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return udf(wrapper, output_type)

    return decorator


class DeIdentified:
    @staticmethod
    @udf_func()
    def hashing(data_input, salt):
        if not data_input:
            return data_input
        data_input_salt = f'{data_input}{salt}'
        return hashlib.md5(data_input_salt.encode('utf-8')).hexdigest()

    @staticmethod
    @udf_func()
    def lower_hashing(data_input, salt):
        if not data_input:
            return data_input
        data_input_salt = f'{str.lower(data_input)}{salt}'
        return hashlib.md5(data_input_salt.encode('utf-8')).hexdigest()

    @staticmethod
    def get_date_format(date_data):
        date_data = str(date_data)
        if Common.is_date_format(date_data, '%Y-%m-%d'):
            date_format = '%Y-%m-%d'
        elif Common.is_date_format(date_data, '%Y-%m-%d %H:%M:%S'):
            date_format = '%Y-%m-%d %H:%M:%S'
        else:
            date_format = '%Y-%m-%d'
        return date_format

    @staticmethod
    @udf_func()
    def birth_date_masking(birth_date, date_format=None):
        if not birth_date:
            return None
        if birth_date == Constant.STRING_NONE_VALUE:
            return birth_date
        if not date_format:
            date_format = DeIdentified.get_date_format(birth_date)
        today_obj = date.today()
        birth_date_obj = None
        try:
            birth_date_obj = datetime.strptime(birth_date, date_format)
        except Exception:
            pass
        try:
            birth_date_obj = birth_date_obj or parser.parse(birth_date)
        except Exception:
            return str(birth_date)

        age = today_obj.year - birth_date_obj.year
        birth_date_year = birth_date_obj.year
        if age >= 90:
            birth_date_year = today_obj.year - 90
        masked_birth_date = date(birth_date_year, 7, 1)
        return masked_birth_date.strftime(date_format)

    @staticmethod
    @udf_func()
    def redaction(date_input):
        return None

    @staticmethod
    @udf_func()
    def postal_code_masking(postal_code):
        postal_code = str(postal_code)
        if not postal_code:
            return None
        if postal_code == Constant.STRING_NONE_VALUE:
            return postal_code
        if len(postal_code) < 3:
            return postal_code
        first_three_digit = postal_code[:3]
        if first_three_digit in ['102', '202', '202', '203', '203', '204', '204', '204', '204', '205', '205', '205',
                                 '205',
                                 '753', '753', '753', '772', '772', '772']:
            first_three_digit = '000'
        if len(postal_code) > 3:
            return f'{first_three_digit}00'
        return first_three_digit

    @staticmethod
    @udf_func()
    def default_hashing(data_input, salt):
        if not data_input:
            return None
        if data_input == Constant.STRING_NONE_VALUE:
            return data_input
        if str(data_input).isnumeric():
            return data_input
        if Common.is_date_format(str(data_input), '%Y-%m-%d') or Common.is_date_format(str(data_input),
                                                                                       '%Y-%m-%d %H:%M:%S'):
            default_date_masking = DeIdentified.birth_date_masking.__wrapped__
            return default_date_masking(data_input)
        default_string_hashing = DeIdentified.hashing.__wrapped__
        return default_string_hashing(data_input, salt)

    @staticmethod
    def de_identify_data_frame(dataframe, de_identify_dict_list, salt):
        for de_identify_dict in de_identify_dict_list:
            column_name = de_identify_dict['column']
            method = de_identify_dict['method']
            if method not in Constant.DE_IDENTIFY_METHOD_LIST:
                continue
            if method == 'hashing':
                dataframe = dataframe.withColumn(column_name,
                                                 DeIdentified.hashing(col(column_name), lit(salt)))
            elif method == 'lower_hashing':
                dataframe = dataframe.withColumn(column_name,
                                                 DeIdentified.lower_hashing(col(column_name), lit(salt)))
            elif method == 'default':
                dataframe = dataframe.withColumn(column_name,
                                                 DeIdentified.default_hashing(col(column_name), lit(salt)))
            else:
                dataframe = dataframe.withColumn(column_name,
                                                 getattr(DeIdentified, method)(col(column_name)))
        return dataframe
