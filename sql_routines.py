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

def get_column_full_type_def(type, length, precision):

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

def get_columns_def(columns_def, schema, table, is_type_needed = True):

    cols_def = "   "

    for column in columns_def:
        column_name = column.name
        column_type = column.type
        column_length = column.length
        column_precision = column.precision

        #e.g. for column name plus full type def: "name character varying(500)"
        cols_def += "\"" + column_name.lower() + "\""
        cols_def += " "
        if is_type_needed:
            column_def = get_column_full_type_def(column_type, column_length, column_precision)
            cols_def += column_def
        cols_def += "\n"
        cols_def += " , "

    cols_def = cols_def.rstrip(', ')

    return cols_def

def get_create_table_query(columns_def, schema, table):

    query = ""

    query += " CREATE TABLE "
    query += "\"" + schema.lower() + "\""
    query += "."
    query += "\"" + table.lower() + "\""
    query += "\n(\n"
    query += get_columns_def(columns_def, schema, table)
    query += ")"
    query += " WITH (appendonly=true, orientation=row, compresstype=quicklz)"

    logging.debug("get_create_table_query - \n" + query)
    return query

def get_create_external_table_query(columns_def, schema, table):

    query = ""

    query += " CREATE READABLE EXTERNAL TABLE "
    query += "\"" + schema.lower() + "\""
    query += "."
    query += "\"ext_" + table.lower() + "\""
    query += "\n(\n"
    query += get_columns_def(columns_def, schema, table)
    query += ")"
    query += " LOCATION ('#EXTERNAL_TABLE') \n"
    query += " FORMAT 'TEXT' \n"
    query += " ( HEADER DELIMITER '\\013' NULL AS '\\\\N' ESCAPE AS '\\\\' \n)"
    query += "	LOG ERRORS INTO "
    query += "\"" + schema.lower() + "\""
    query += "."
    query += "\"ext_error_table\" \n"
    query += " SEGMENT REJECT LIMIT 1000 ROWS"


    logging.debug("getExternalCreateTableQuery - \n" + query);
    return query
