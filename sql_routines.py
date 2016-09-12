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

def getSQL(columns_def, schema, table, scd, pk, scdDate):

    sqlStageFullCreate = "CREATE TABLE #TARGET_SCHEMA.#STAGE_FULL_PREFIX#TABLE_NAME \n ( \n #NATURAL_COLS_WITH_TYPES\n ) ";

    sqlDWHtableCreate = """CREATE TABLE #TARGET_SCHEMA.h_#TABLE_NAME \n" +
                        "( \n" +
                        "  p_id serial,\n" +
                        "  #NATURAL_COLS_WITH_TYPES,\n" +
                        "  p_active_flag character varying(1),\n" +
                        "  p_valid_from timestamp without time zone,\n" +
                        "  p_valid_to timestamp without time zone \n" +
                        ") " +
                        "WITH (appendonly=true, orientation=row, compresstype=quicklz)"""


    sqlDWHviewCreate =  """CREATE VIEW #TARGET_SCHEMA.#TABLE_NAME AS \n" +
    							"SELECT \n" +
    							"#NATURAL_COLS_WITHOUT_TYPES_WO_QUAL\n" +
    							"FROM #TARGET_SCHEMA.h_#TABLE_NAME \n" +
    							"WHERE p_active_flag='Y' """

    sqlDWHtableUpdateSCD = """UPDATE #TARGET_SCHEMA.h_#TABLE_NAME \n" +
                            "SET \n" +
                            "  p_valid_to=#SYSDATE_MINUS_ONE_DAY, \n" +
                            "  p_active_flag='N' \n" +
                            "FROM \n" +
                            "( \n" +
                            "  SELECT \n" +
                            "    #SQL_TYPE, \n" +
                            "    #IS_EQUAL, \n" +
                            "    #NATURAL_COLS_WITHOUT_TYPES_WITH_H,\n" +
                            "    h_#TABLE_NAME.p_valid_from, \n" +
                            "    h_#TABLE_NAME.p_valid_to, \n" +
                            "    h_#TABLE_NAME.p_active_flag \n" +
                            "  FROM \n" +
                            "    ( \n" +
                            "      SELECT * \n" +
                            "      FROM #TARGET_SCHEMA.h_#TABLE_NAME \n" +
                            "      WHERE p_valid_to='21000101'::DATE::TIMESTAMP \n" +
                            "    ) h_#TABLE_NAME \n" +
                            "    FULL OUTER JOIN #TARGET_SCHEMA.s_#TABLE_NAME \n" +
                            "    ON #PK_JOIN_IN_ON \n" +
                            ") t \n" +
                            "WHERE \n" +
                            "  #PK_JOIN_IN_WHERE AND \n" +
                            "  h_#TABLE_NAME.p_valid_to='21000101'::DATE::TIMESTAMP AND \n" +
                            "  ( \n" +
                            "    t.sql_type='DELETE' OR \n" +
                            "    (t.sql_type='UPDATE' AND is_equal LIKE '%N%') \n" +
                            "  )"""

    sqlDWHtableInsertSCD=    """INSERT INTO #TARGET_SCHEMA.h_#TABLE_NAME \n" +
                                "( \n" +
                              "  #NATURAL_COLS_WITHOUT_TYPES_WO_QUAL,\n" +
                              "  p_active_flag,\n" +
                              "  p_valid_from,\n" +
                              "  p_valid_to\n" +
                              ") \n" +
                              "SELECT \n" +
                              "  #NATURAL_COLS_WITHOUT_TYPES_WITH_T,\n" +
                              "  'Y'::VARCHAR(1) p_active_flag, \n" +
                              " #P_VALID_FROM_CREDATE ,\n" +
                              "  '21000101'::DATE::TIMESTAMP p_valid_to \n" +
                              "FROM \n" +
                              "( \n" +
                              "  SELECT \n" +
                              "    #SQL_TYPE, \n" +
                              "    #IS_EQUAL, \n" +
                              "    #NATURAL_COLS_WITHOUT_TYPES_WITH_S,\n" +
                              "    h_#TABLE_NAME.p_valid_from, \n" +
                              "    h_#TABLE_NAME.p_valid_to, \n" +
                              "    h_#TABLE_NAME.p_active_flag \n" +
                              "  FROM \n" +
                              "    ( \n" +
                              "		select * from ( \n" +
                              "		  select * ,row_number() OVER (PARTITION BY " +pk+" ORDER BY p_valid_from DESC) as p_rn \n" +
                              "		  from #TARGET_SCHEMA.h_#TABLE_NAME ) tmp_h_#TABLE_NAME \n" +
                              "		  WHERE p_rn = 1 \n" +

                              "    ) h_#TABLE_NAME \n" +
                              "    FULL OUTER JOIN #TARGET_SCHEMA.s_#TABLE_NAME \n" +
                              "    ON #PK_JOIN_IN_ON \n" +
                              ") t \n" +
                              "WHERE \n" +
                              "  t.sql_type='INSERT' OR \n" +
                              "  (t.sql_type='UPDATE' AND is_equal LIKE '%N%')"""

    colPK_JOIN_IN_ON_Template = "h_#TABLE.#PK_PART=s_#TABLE.#PK_PART"
    colPK_JOIN_IN_WHERE_Template = "h_#TABLE.#PK_PART=t.#PK_PART"

    colSCD_ActPrevListWithoutTypes_String = ""
    colSCD_ActPrevListWithTypes_String = ""
    colSCD_SqlType_String = ""
    colSCD_IsEqual_String = ""
    colSCD_ActPrevDef_String = ""
    colPK_JOIN_IN_ON_String = ""
    colPK_JOIN_IN_WHERE_String = ""

    strSTAGE_FULL_PREFIX = ""
    strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL = ""
    strNATURAL_COLS_WITHOUT_TYPES_WITH_H = ""
    strNATURAL_COLS_WITHOUT_TYPES_WITH_S = ""
    strNATURAL_COLS_WITHOUT_TYPES_WITH_T = ""
    strNATURAL_COLS_WITH_TYPES = ""
    strACT_PREV_DEF = ""
    strSQL_TYPE = ""
    strIS_EQUAL = ""
    strACT_PREV_LIST_WITHOUT_TYPES = ""
    strACT_PREV_LIST_WITH_TYPES = ""
    strPK_JOIN_IN_ON = ""
    strPK_JOIN_IN_WHERE = ""
    strP_ID_WITHOUT_TYPE = ""
    strP_ID_WITH_TYPE = ""
    strP_ACTIVE_FLAG_WITHOUT_TYPE = "p_active_flag"
    strP_ACTIVE_FLAG_WITH_TYPE = "p_valid_flag varchar(1)"
    strP_VALID_FROM_WITHOUT_TYPE = "p_valid_from"
    strP_VALID_FROM_WITH_TYPE = "p_valid_from timestamptz"
    strP_VALID_TO_WITHOUT_TYPE = "p_valid_to"
    strP_VALID_TO_WITH_TYPE = "p_valid_to timestamptz"
    strSYSDATE_MINUS_ONE_DAY = ""
    strDATE_INFINITE = ""
    strSYSDATE = ""
    strYES_FLAG = ""

    logging.debug("pk: " + pk);
    strPK_JOIN_IN_ON = "";
    strPK_JOIN_IN_WHERE = "";





