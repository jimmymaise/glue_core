from jinja2 import contextfilter, Markup


@contextfilter
def subrender(context, value):
    _template = context.eval_ctx.environment.from_string(value)
    result = _template.render(**context)
    if context.eval_ctx.autoescape:
        result = Markup(result)
    return result


def normalize_column_name(column_name):
    normalize_column_list = []
    for normalize_column in normalize_column_list:
        if normalize_column.lower() == column_name.lower():
            return f'"{column_name}"'
    return column_name


def get_schema_name(table_with_schema_name):
    return table_with_schema_name.split('.')[0]


def get_table_name(table_with_schema_name):
    if '.' not in table_with_schema_name:
        return table_with_schema_name
    return table_with_schema_name.split('.')[1]
