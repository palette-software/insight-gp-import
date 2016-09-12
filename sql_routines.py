import logging

class ColumnMetadata(object):

    def __init__(self):
        self.schema = ""
        self.table = ""
        self.attnum = -1
        self.name = ""
        self.type = ""
        self.length = 0
        self.precision = 0

def get_column_def(type, length, precision):

    query = ""
    column_def = ""
    column_def += type
    query += " "

    if length > 0 and precision > 0:
        column_def += "(" + length + "," + precision + ")"
    elif length > 0 and precision == 0:
        column_def += "(" + length + ")"
    # Workaround for the external table issue with GP
    elif type.lower() == "numeric":
        column_def += "(28,7)"

    return column_def

def get_create_table_query(columns, schema, table):

    query = ""

    query += " CREATE TABLE "
    query += "\"" + schema.lower() + "\""
    query += "."
    query += "\"" + table.lower() + "\""
    query += "\n(\n"
    query += "   "

    for column in columns:
        column_name = column.name
        column_type = column.type
        column_length = column.length
        column_precision = column.precision

        query += "\"" + column_name.lower() + "\""
        query += " "
        column_def = get_column_def(column_type, column_length, column_precision)
        query += column_def + "\n"
        query += " , "

    query += query.rstrip(', ') + ")"
    query += " WITH (appendonly=true, orientation=row, compresstype=quicklz)"

    logging.debug("get_create_table_query - " + query)
    return query