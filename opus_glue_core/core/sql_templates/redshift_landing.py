TEMPLATE = """
-- SQL_BLOCK_CODE: create_table_from_column_list
-- Comment
drop table if exists {{schema_name}}.{{table_name}};
create table {{schema_name}}.{{table_name}}
(
    {% for item in column_dict_list %}
        "{{item.column_name|normalize_column_name}}" {{item.data_type}} {{ "," if not loop.last }}
	 {% endfor %}
);
"""
