import logging
import datetime


class SqlRoutines(object):
    def __init__(self, db, schema):
        self._db = db
        self._schema = schema

    def get_table_columns_def_from_db(self, table):
        sql = """SELECT n.nspname as schemaname, c.relname as tablename,
                        a.attname as columnname,
                        format_type(a.atttypid, a.atttypmod),
                        a.attnum
                        FROM pg_namespace n
                          JOIN pg_class c ON (n.oid = c.relnamespace)
                          JOIN pg_attribute a ON (c.oid = a.attrelid)
                          JOIN pg_type t ON (a.atttypid = t.oid)
                        WHERE 1 = 1
                        AND nspname = %(schema)s
                        AND c.relname = %(table)s
                        AND a.attnum > 0 /*filter out the internal columns*/
                        ORDER BY n.nspname,c.relname,a.attnum ASC"""

        params = {'schema': self._schema, 'table': table}
        return self._db.execute_in_transaction(sql, params)

    def gen_alter_cols_because_of_metadata_change(self, table, columns_def, incremental=True):
        # TODO type also should be checked
        sql_alter_stmts = []
        table_prefix = "h_" if not incremental else ""
        cols_def_from_db = self.get_table_columns_def_from_db(table_prefix + table)

        only_col_names = [cd[2] for cd in cols_def_from_db]

        for col_def in columns_def:
            if col_def["name"] not in only_col_names:
                sql_stmt = "ALTER TABLE {schema_name}.{table_type}{table_name} ADD COLUMN " + col_def["name"] + " " + \
                           col_def["type"] + "  default null;\n"
                if incremental:
                    sql_alter_stmts.append(sql_stmt.format(schema_name=self._schema, table_name=table, table_type=""))
                else:
                    sql_alter_stmts.append(sql_stmt.format(schema_name=self._schema, table_name=table, table_type="s_"))
                    sql_alter_stmts.append(sql_stmt.format(schema_name=self._schema, table_name=table, table_type="h_"))

        return sql_alter_stmts

    def table_exists(self, table):
        sql = """SELECT COALESCE((  select table_name
                                    from
                                        information_schema.tables
                                    where
                                        lower(table_schema) = lower(%(schema)s) AND
                                        lower(table_name) = lower(%(table)s)
                ), 'MISSING_TABLE')"""

        params = {'schema': self._schema, 'table': table}
        result = self._db.execute_in_transaction(sql, params)
        if result[0][0] == "MISSING_TABLE":
            return False

        return True

    def get_column_full_type_def(self, type, length, precision):
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

    def get_columns_def(self, columns_def, table, type_needed=True, p_id_needed=True):
        cols_def = ""
        if p_id_needed:
            if type_needed:
                cols_def += ", \"p_id\" bigserial\n"
            else:
                cols_def += ", \"p_id\"\n"

        if type_needed:
            cols_def += ", \"p_filepath\" varchar (500)\n"
        else:
            cols_def += ", \"p_filepath\"\n"

        for column in columns_def:
            column_name = column["name"]
            column_type = column["type"]
            column_length = column["length"]
            column_precision = column["precision"]

            # e.g. for column name plus full type def: "name character varying(500)"
            cols_def += " , \"" + column_name.lower() + "\""
            cols_def += " "
            if type_needed:
                column_def = self.get_column_full_type_def(column_type, column_length, column_precision)
                cols_def += column_def
            cols_def += "\n"

        cols_def = cols_def.lstrip(",")

        if type_needed:
            cols_def += " , \"p_cre_date\" timestamp without time zone \n"
        else:
            cols_def += " , \"p_cre_date\" \n"

        return cols_def

    def get_create_incremental_table_query(self, columns_def, table):
        query = ""

        query += " CREATE TABLE "
        query += "\"" + self._schema.lower() + "\""
        query += "."
        query += "\"" + table.lower() + "\""
        query += "\n(\n"
        query += self.get_columns_def(columns_def, table, table)
        query += ")"
        query += " WITH (appendonly=true, orientation=row, compresstype=quicklz)"

        logging.debug("get_create_incremental_table_query - \n" + query)
        return query

    def get_create_external_table_query(self, columns_def, table):
        query = ""

        query += " CREATE READABLE EXTERNAL TABLE "
        query += "\"" + self._schema.lower() + "\""
        query += "."
        query += "\"ext_" + table.lower() + "\""
        query += "\n(\n"
        query += self.get_columns_def(columns_def, table, True, False)
        query += ")"
        query += " LOCATION ('#EXTERNAL_TABLE') \n"
        query += " FORMAT 'TEXT' \n"
        query += " ( HEADER DELIMITER '\\013' NULL AS '\\\\N' ESCAPE AS '\\\\' \n)"
        query += "	LOG ERRORS INTO "
        query += "\"" + self._schema.lower() + "\""
        query += "."
        query += "\"ext_error_table\" \n"
        query += " SEGMENT REJECT LIMIT 1000 ROWS"

        logging.debug("getExternalCreateTableQuery - \n" + query)
        return query

    def add_filepath_credate_to_coldef(self, coldef, table):
        cm1 = {"schema": self._schema, "table": table, "name": 'p_filepath', "type": 'VARCHAR (500)', "length": 0,
               "precision": 0}

        cm2 = {"schema": self._schema, "table": table, "name": 'p_cre_date', "type": 'TIMESTAMP WITHOUT TIME ZONE',
               "length": 0, "precision": 0}

        new_coldef = coldef[:]

        new_coldef.append(cm1)
        new_coldef.append(cm2)

        return new_coldef

    def drop_table(self, table, external=False):
        external = "external" if external else ""
        self._db.execute_non_query_in_transaction(
            "drop {external} table if exists {schema_name}.{table_name}".format(schema_name=self._schema,
                                                                                table_name=table,
                                                                                external=external))

    def getSQL(self, columns_def, table, scd, pk, scdDate):
        sqlStageFullCreate = "CREATE TABLE #TARGET_SCHEMA.#STAGE_FULL_PREFIX#TABLE_NAME \n ( \n #NATURAL_COLS_WITH_TYPES\n ) "

        sqlDWHtableCreate = """CREATE TABLE #TARGET_SCHEMA.h_#TABLE_NAME \n
                            ( \n
                              p_id serial,\n
                              #NATURAL_COLS_WITH_TYPES,\n
                              p_active_flag character varying(1),\n
                              p_valid_from timestamp without time zone,\n
                              p_valid_to timestamp without time zone \n
                            )
                            WITH (appendonly=true, orientation=row, compresstype=quicklz)"""

        sqlDWHviewCreate = """CREATE VIEW #TARGET_SCHEMA.#TABLE_NAME AS \n
                                    SELECT \n
                                    #NATURAL_COLS_WITHOUT_TYPES_WO_QUAL\n
                                    FROM #TARGET_SCHEMA.h_#TABLE_NAME \n
                                    WHERE p_active_flag='Y' """

        sqlDWHtableUpdateSCD = """UPDATE #TARGET_SCHEMA.h_#TABLE_NAME \n
                                SET \n
                                  p_valid_to=#SYSDATE_MINUS_ONE_DAY, \n
                                  p_active_flag='N' \n
                                FROM \n
                                ( \n
                                  SELECT \n
                                    #SQL_TYPE, \n
                                    #IS_EQUAL, \n
                                    #NATURAL_COLS_WITHOUT_TYPES_WITH_H,\n
                                    h_#TABLE_NAME.p_valid_from, \n
                                    h_#TABLE_NAME.p_valid_to, \n
                                    h_#TABLE_NAME.p_active_flag \n
                                  FROM \n
                                    ( \n
                                      SELECT * \n
                                      FROM #TARGET_SCHEMA.h_#TABLE_NAME \n
                                      WHERE p_valid_to='21000101'::DATE::TIMESTAMP \n
                                    ) h_#TABLE_NAME \n
                                    FULL OUTER JOIN #TARGET_SCHEMA.s_#TABLE_NAME \n
                                    ON #PK_JOIN_IN_ON \n
                                ) t \n
                                WHERE \n
                                  #PK_JOIN_IN_WHERE AND \n
                                  h_#TABLE_NAME.p_valid_to='21000101'::DATE::TIMESTAMP AND \n
                                  ( \n
                                    t.sql_type='DELETE' OR \n
                                    (t.sql_type='UPDATE' AND is_equal LIKE '%N%') \n
                                  )"""

        sqlDWHtableInsertSCD = """INSERT INTO #TARGET_SCHEMA.h_#TABLE_NAME \n
                                    ( \n
                                    #NATURAL_COLS_WITHOUT_TYPES_WO_QUAL,\n
                                    p_active_flag,\n
                                    p_valid_from,\n
                                    p_valid_to\n
                                  ) \n
                                  SELECT \n
                                    #NATURAL_COLS_WITHOUT_TYPES_WITH_T,\n
                                    'Y'::VARCHAR(1) p_active_flag, \n
                                   #P_VALID_FROM_CREDATE ,\n
                                    '21000101'::DATE::TIMESTAMP p_valid_to \n
                                  FROM \n
                                  ( \n
                                    SELECT \n
                                      #SQL_TYPE, \n
                                      #IS_EQUAL, \n
                                      #NATURAL_COLS_WITHOUT_TYPES_WITH_S,\n
                                      h_#TABLE_NAME.p_valid_from, \n
                                      h_#TABLE_NAME.p_valid_to, \n
                                      h_#TABLE_NAME.p_active_flag \n
                                    FROM \n
                                      ( \n
                                        select * from ( \n
                                          select * ,row_number() OVER (PARTITION BY {pk} ORDER BY p_valid_from DESC) as p_rn \n
                                          from #TARGET_SCHEMA.h_#TABLE_NAME ) tmp_h_#TABLE_NAME \n
                                          WHERE p_rn = 1 \n

                                      ) h_#TABLE_NAME \n
                                      FULL OUTER JOIN #TARGET_SCHEMA.s_#TABLE_NAME \n
                                      ON #PK_JOIN_IN_ON \n
                                  ) t \n
                                  WHERE \n
                                    t.sql_type='INSERT' OR \n
                                    (t.sql_type='UPDATE' AND is_equal LIKE '%N%')""".format(pk=', '.join(pk))

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

        logging.debug("pk: " + str(pk))
        strPK_JOIN_IN_ON = ""
        strPK_JOIN_IN_WHERE = ""

        for pk_part in pk:
            logging.debug("pk_part: " + pk_part)
            colPK_JOIN_IN_ON_String = colPK_JOIN_IN_ON_Template
            colPK_JOIN_IN_ON_String = colPK_JOIN_IN_ON_String.replace("#TABLE", table)
            colPK_JOIN_IN_ON_String = colPK_JOIN_IN_ON_String.replace("#PK_PART", pk_part) + " AND "
            colPK_JOIN_IN_WHERE_String = colPK_JOIN_IN_WHERE_Template
            colPK_JOIN_IN_WHERE_String = colPK_JOIN_IN_WHERE_String.replace("#TABLE", table)
            colPK_JOIN_IN_WHERE_String = colPK_JOIN_IN_WHERE_String.replace("#PK_PART", pk_part) + " AND "

            strPK_JOIN_IN_ON = strPK_JOIN_IN_ON + colPK_JOIN_IN_ON_String
            strPK_JOIN_IN_WHERE = strPK_JOIN_IN_WHERE + colPK_JOIN_IN_WHERE_String

        logging.debug("strPK_JOIN_IN_ON: " + strPK_JOIN_IN_ON)
        logging.debug("strPK_JOIN_IN_WHERE: " + strPK_JOIN_IN_WHERE)
        strPK_JOIN_IN_ON = strPK_JOIN_IN_ON[0: len(strPK_JOIN_IN_ON) - 5]
        strPK_JOIN_IN_WHERE = strPK_JOIN_IN_WHERE[0: len(strPK_JOIN_IN_WHERE) - 5]
        logging.debug("strPK_JOIN_IN_ON: " + strPK_JOIN_IN_ON)
        logging.debug("strPK_JOIN_IN_WHERE: " + strPK_JOIN_IN_WHERE)

        # columns
        strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL = ""
        strNATURAL_COLS_WITHOUT_TYPES_WITH_H = ""
        strNATURAL_COLS_WITHOUT_TYPES_WITH_S = ""
        strNATURAL_COLS_WITHOUT_TYPES_WITH_T = ""
        strNATURAL_COLS_WITH_TYPES = ""
        strACT_PREV_LIST_WITHOUT_TYPES = ""
        strACT_PREV_LIST_WITH_TYPES = ""
        strSQL_TYPE = ""
        strIS_EQUAL = ""
        strACT_PREV_DEF = ""

        columns_def_extended = self.add_filepath_credate_to_coldef(columns_def, table)

        for i in range(len(columns_def_extended)):
            column = columns_def_extended[i]
            column_name = column["name"]
            if ("," + column_name.upper() + ",") not in ",P_ID,P_ACTIVE_FLAG,P_VALID_FROM,P_VALID_TO,":
                strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL = strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL + "  " + column_name.lower()
                strNATURAL_COLS_WITHOUT_TYPES_WITH_H = strNATURAL_COLS_WITHOUT_TYPES_WITH_H + "  h_" + table + "." + column_name.lower()
                strNATURAL_COLS_WITHOUT_TYPES_WITH_S = strNATURAL_COLS_WITHOUT_TYPES_WITH_S + "  s_" + table + "." + column_name.lower()
                strNATURAL_COLS_WITHOUT_TYPES_WITH_T = strNATURAL_COLS_WITHOUT_TYPES_WITH_T + "  t." + column_name.lower()
                strNATURAL_COLS_WITH_TYPES = strNATURAL_COLS_WITH_TYPES + "  " + column_name.lower() + " " + self.get_column_full_type_def(
                    column["type"], column["length"], column["precision"])

                colSCD_ActPrevListWithoutTypes_String = "  act_#COL, \n  prev_#COL".replace("#TABLE", table).replace(
                    "#COL",
                    column_name.lower())
                colSCD_ActPrevListWithTypes_String = "  act_#COL #DATA_TYPE, \n  prev_#COL #DATA_TYPE".replace("#TABLE",
                                                                                                               table).replace(
                    "#COL", column_name.lower()).replace("#DATA_TYPE",
                                                         self.get_column_full_type_def(column["type"], column["length"],
                                                                                       column["precision"]))

                colSCD_SqlType_String = """    CASE \n
                      WHEN h_#TABLE.#COL IS NULL THEN 'INSERT' \n
                      WHEN s_#TABLE.#COL IS NULL THEN 'DELETE' \n
                      WHEN h_#TABLE.#COL IS NOT NULL AND s_#TABLE.#COL IS NOT NULL AND h_#TABLE.#COL=s_#TABLE.#COL THEN 'UPDATE' \n
                      ELSE 'N/A' \n
                    END sql_type"""
                colSCD_SqlType_String = colSCD_SqlType_String.replace("#TABLE", table).replace("#COL",
                                                                                               column_name.lower())

                colSCD_IsEqual_String = """    CASE \n
                      WHEN s_#TABLE.#COL IS NULL AND h_#TABLE.#COL IS NULL THEN 'Y' \n
                      WHEN s_#TABLE.#COL IS NOT NULL AND h_#TABLE.#COL IS NOT NULL AND s_#TABLE.#COL=h_#TABLE.#COL THEN 'Y' \n
                      ELSE 'N' \n
                    END """
                colSCD_IsEqual_String = colSCD_IsEqual_String.replace("#TABLE", table).replace("#COL",
                                                                                               column_name.lower())

                if column_name.lower() == "p_cre_date" or column_name.lower() == "p_filepath":
                    colSCD_IsEqual_String = ""

                colSCD_ActPrevDef_String = "    s_#TABLE.#COL act_#COL, \n    h_#TABLE.act_#COL prev_#COL"
                colSCD_ActPrevDef_String = colSCD_ActPrevDef_String.replace("#TABLE", table).replace("#COL",
                                                                                                     column_name.lower())

                for pk_part in pk:
                    if pk_part.lower() == column_name.lower():
                        strACT_PREV_LIST_WITHOUT_TYPES = strACT_PREV_LIST_WITHOUT_TYPES + "  " + pk_part
                        strACT_PREV_LIST_WITH_TYPES = strACT_PREV_LIST_WITH_TYPES + "  #COL #DATA_TYPE".replace("#COL",
                                                                                                                pk_part).replace(
                            "#DATA_TYPE",
                            self.get_column_full_type_def(column["type"], column["length"], column["precision"]))
                        strSQL_TYPE = colSCD_SqlType_String.replace("#COL", pk_part)
                        strACT_PREV_DEF = strACT_PREV_DEF + "    s_#TABLE.#COL".replace("#TABLE", table).replace("#COL",
                                                                                                                 pk_part)
                    else:
                        strACT_PREV_LIST_WITHOUT_TYPES = strACT_PREV_LIST_WITHOUT_TYPES + colSCD_ActPrevListWithoutTypes_String
                        strACT_PREV_LIST_WITH_TYPES = strACT_PREV_LIST_WITH_TYPES + colSCD_ActPrevListWithTypes_String
                        strIS_EQUAL = strIS_EQUAL + colSCD_IsEqual_String
                        strACT_PREV_DEF = strACT_PREV_DEF + colSCD_ActPrevDef_String

            if i != len(columns_def_extended) - 1:
                strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL = strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL + ", \n"
                strNATURAL_COLS_WITHOUT_TYPES_WITH_H = strNATURAL_COLS_WITHOUT_TYPES_WITH_H + ", \n"
                strNATURAL_COLS_WITHOUT_TYPES_WITH_S = strNATURAL_COLS_WITHOUT_TYPES_WITH_S + ", \n"
                strNATURAL_COLS_WITHOUT_TYPES_WITH_T = strNATURAL_COLS_WITHOUT_TYPES_WITH_T + ", \n"
                strNATURAL_COLS_WITH_TYPES = strNATURAL_COLS_WITH_TYPES + ", \n"

                strACT_PREV_LIST_WITHOUT_TYPES = strACT_PREV_LIST_WITHOUT_TYPES + ", \n"
                strACT_PREV_LIST_WITH_TYPES = strACT_PREV_LIST_WITH_TYPES + ", \n"

                if strIS_EQUAL.replace("\n", "").strip().endswith("||") == False:
                    strIS_EQUAL = strIS_EQUAL + " || \n"

                strACT_PREV_DEF = strACT_PREV_DEF + ", \n"

        strACT_PREV_LIST_WITHOUT_TYPES = strACT_PREV_LIST_WITHOUT_TYPES[2:]
        strACT_PREV_LIST_WITH_TYPES = strACT_PREV_LIST_WITH_TYPES[2:]

        if len(strSQL_TYPE) > 4:
            strSQL_TYPE = strSQL_TYPE[4:]

        strIS_EQUAL = strIS_EQUAL[4:] + "'' is_equal"
        strACT_PREV_DEF = strACT_PREV_DEF[4:]

        if scd.lower() == "yes":
            strSTAGE_FULL_PREFIX = "s_"

        strP_ID_WITHOUT_TYPE = "p_id"
        strP_ID_WITH_TYPE = "p_id serial"

        strP_ACTIVE_FLAG_WITH_TYPE = "p_active_flag character varying(1)"
        strP_VALID_FROM_WITH_TYPE = "p_valid_from timestamp without time zone"
        strP_VALID_TO_WITH_TYPE = "p_valid_to timestamp without time zone"

        if scdDate is None or len(scdDate) < 0:
            scdDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        strSYSDATE_MINUS_ONE_DAY = "('" + scdDate + "'::timestamp -INTERVAL '1' second)::TIMESTAMP"
        strDATE_INFINITE = "'21000101'::DATE::TIMESTAMP"
        strSYSDATE = "'" + scdDate + "'::TIMESTAMP"
        strYES_FLAG = "'Y'::VARCHAR(1)"

        sqlStageFullCreate = sqlStageFullCreate.replace("#TARGET_SCHEMA", self._schema)
        sqlStageFullCreate = sqlStageFullCreate.replace("#TABLE_NAME", table)
        sqlStageFullCreate = sqlStageFullCreate.replace("#STAGE_FULL_PREFIX", strSTAGE_FULL_PREFIX)
        sqlStageFullCreate = sqlStageFullCreate.replace("#NATURAL_COLS_WITH_TYPES", strNATURAL_COLS_WITH_TYPES)

        sqlDWHtableCreate = sqlDWHtableCreate.replace("#TARGET_SCHEMA", self._schema)
        sqlDWHtableCreate = sqlDWHtableCreate.replace("#TABLE_NAME", table)
        sqlDWHtableCreate = sqlDWHtableCreate.replace("#NATURAL_COLS_WITH_TYPES", strNATURAL_COLS_WITH_TYPES)

        sqlDWHviewCreate = sqlDWHviewCreate.replace("#TARGET_SCHEMA", self._schema)
        sqlDWHviewCreate = sqlDWHviewCreate.replace("#TABLE_NAME", table)
        sqlDWHviewCreate = sqlDWHviewCreate.replace("#NATURAL_COLS_WITHOUT_TYPES_WO_QUAL",
                                                    strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL)

        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#TARGET_SCHEMA", self._schema)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#TABLE_NAME", table)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#SQL_TYPE", strSQL_TYPE)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#IS_EQUAL", strIS_EQUAL)

        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WO_QUAL",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WITH_H",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WITH_H)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#PK_JOIN_IN_ON", strPK_JOIN_IN_ON)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#PK_JOIN_IN_WHERE", strPK_JOIN_IN_WHERE)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#SYSDATE_MINUS_ONE_DAY", strSYSDATE_MINUS_ONE_DAY)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#DATE_INFINITE", strDATE_INFINITE)
        sqlDWHtableUpdateSCD = sqlDWHtableUpdateSCD.replace("#SYSDATE", strSYSDATE)

        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#TARGET_SCHEMA", self._schema)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#TABLE_NAME", table)

        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WO_QUAL",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WO_QUAL)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WITH_H",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WITH_H)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WITH_S",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WITH_S)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#NATURAL_COLS_WITHOUT_TYPES_WITH_T",
                                                            strNATURAL_COLS_WITHOUT_TYPES_WITH_T)

        forReplace = ""
        if "created_at" in sqlDWHtableInsertSCD:
            forReplace = "  CASE WHEN t.sql_type='INSERT' THEN coalesce(t.created_at,'10010101'::date::timestamp) ELSE #SYSDATE END p_valid_from"
        else:
            forReplace = "  CASE WHEN t.sql_type='INSERT' THEN '10010101'::date::timestamp ELSE #SYSDATE END p_valid_from"

        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#P_VALID_FROM_CREDATE", forReplace)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#SQL_TYPE", strSQL_TYPE)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#IS_EQUAL", strIS_EQUAL)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#PK_JOIN_IN_ON", strPK_JOIN_IN_ON)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#SYSDATE_MINUS_ONE_DAY", strSYSDATE_MINUS_ONE_DAY)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#DATE_INFINITE", strDATE_INFINITE)
        sqlDWHtableInsertSCD = sqlDWHtableInsertSCD.replace("#SYSDATE", strSYSDATE)

        map = {}
        map['StageFullCreate'] = sqlStageFullCreate
        map['DWHtableCreate'] = sqlDWHtableCreate
        map['DWHviewCreate'] = sqlDWHviewCreate
        map['DWHtableUpdateSCD'] = sqlDWHtableUpdateSCD
        map['DWHtableInsertSCD'] = sqlDWHtableInsertSCD

        logging.debug("StageFullCreate - " + sqlStageFullCreate)
        logging.debug("DWHtableCreate - " + sqlDWHtableCreate)
        logging.debug("DWHviewCreate - " + sqlDWHviewCreate)
        logging.debug("DWHtableUpdateSCD - " + sqlDWHtableUpdateSCD)
        logging.debug("DWHtableInsertSCD - " + sqlDWHtableInsertSCD)
        return map

    def manage_partitions(self, table):
        if table in ("threadinfo", "serverlogs", "plainlogs"):
            query = ("select {schema_name}.manage_partitions('{schema_name}', '{table_name}')").format(
                schema_name=self._schema,
                table_name=table)
            self._db.execute_in_transaction(query)

    def get_insert_data_from_external_table_query(self, metadata, src_table, trg_table, day=None):
        query = "INSERT INTO {schema_name}.{trg_table_name} ( \n" + \
                self.get_columns_def(metadata, src_table, type_needed=False, p_id_needed=False) + \
                " ) \n" \
                "SELECT \n" + \
                self.get_columns_def(metadata, src_table, type_needed=False, p_id_needed=False) + \
                " FROM {schema_name}.{src_table_name}"

        if trg_table == "threadinfo":
            if day is None:
                raise Exception("Day parameter cannot be None in case of threadinfo")
            query += " WHERE 1 = 1" \
                     "      and ts >= date'{day}'" \
                     "      and ts < date'{day}' + 1"

        if trg_table in ("http_requests", "background_jobs", "async_jobs"):
            if day is None:
                raise Exception("Day parameter cannot be None in case of {}".format(trg_table))
            query += " WHERE 1 = 1" \
                     "      and created_at >= date'{day}'" \
                     "      and created_at < date'{day}' + 1"

        query = query.format(schema_name=self._schema, src_table_name=src_table, trg_table_name=trg_table, day=day)
        return query

    def get_distinct_days_from_table(self, table, column):
        query = "select distinct to_char({column_name}::date, 'yyyy-mm-dd') as day from {schema_name}.{table_name} order by 1".format(schema_name=self._schema, column_name=column, table_name=table)
        result = self._db.execute_in_transaction(query)
        return result

    def insert_data_from_external_table(self, metadata, src_table, trg_table):
        logging.info("Start loading data from external table - From: {}, To: {}".format(src_table, trg_table))
        sqls = []
        if trg_table in ("threadinfo", "http_requests", "background_jobs", "async_jobs"):
            if trg_table == "threadinfo":
                column_name = "ts"
            else:
                column_name = "created_at"

            rows = self.get_distinct_days_from_table("ext_" + trg_table, column_name)

            for row in rows:
                day = row[0]
                sql = self.get_insert_data_from_external_table_query(metadata, src_table, trg_table, day)
                sqls.append(sql)
        else:
            sql = self.get_insert_data_from_external_table_query(metadata, src_table, trg_table)
            sqls.append(sql)
        result = self._db.execute_non_query_in_transaction(sqls)
        logging.info(
            "End loading data from external table - From: {}, To: {}. Inserted = {}".format(src_table, trg_table, result))

    def apply_scd(self, metadata_for_table, table, scd_date, pk):
        logging.info("Start applying SCD. Table = {}, SCD Date: {}".format(table, scd_date))
        sql_queries_map = self.getSQL(metadata_for_table, table, "yes", pk, scd_date)

        queries_in_transaction = [sql_queries_map["DWHtableUpdateSCD"], sql_queries_map["DWHtableInsertSCD"]]
        upd_ins_rowcount = self._db.execute_non_query_in_transaction(queries_in_transaction)
        logging.info("End applying SCD. Table = {}, Updated = {}, Inserted = {}".format(table, upd_ins_rowcount[0],
                                                                                        upd_ins_rowcount[1]))

    def load_data_from_external_table(self, metadata_for_table, table):
        query = 'truncate table {schema_name}.s_{table_name}'.format(schema_name=self._schema, table_name=table)
        self._db.execute_non_query_in_transaction(query)
        src_table = "ext_" + table
        trg_table = "s_" + table
        self.insert_data_from_external_table(metadata_for_table, src_table, trg_table)

    def recreate_external_table(self, table, metadata_for_table, gpfdist_addr, incremental):
        ext_table = "ext_" + table
        ext_table_create_sql = self.get_create_external_table_query(metadata_for_table, table)
        ext_table_create_sql = ext_table_create_sql.replace("#EXTERNAL_TABLE",
                                                            "gpfdist://{gpfdist_addr}/*/{table_name}-*.csv.gz".format(
                                                                gpfdist_addr=gpfdist_addr, table_name=table))

        self.drop_table(ext_table, external=True)
        self._db.execute_non_query_in_transaction(ext_table_create_sql)
        logging.debug("External table recreated: {table_name}".format(table_name=ext_table))

    def alter_dwh_table_if_needed(self, alter_list):
        self._db.execute_non_query_in_transaction(alter_list)

    def create_dwh_full_tables_if_needed(self, table, sql_queries_map):
        if not self.table_exists("h_" + table):
            self._db.execute_non_query_in_transaction(sql_queries_map["DWHtableCreate"])
            self._db.execute_non_query_in_transaction(sql_queries_map["StageFullCreate"])
            return True
        return False

    def create_dwh_incremantal_tables_if_needed(self, table, metadata_for_table):
        if not self.table_exists(table):
            sql = self.get_create_incremental_table_query(metadata_for_table, table)
            self._db.execute_non_query_in_transaction(sql)
            return True
        return False
