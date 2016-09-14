import logging
import logging.handlers
import yaml
import sys
import gzip
import sql_routines as sr
from database import Database
import os
import re
import shutil
from time import sleep

FATAL_ERROR = 49


class PaletteFileParseError(Exception):
    pass


def get_latest_metadata_file():
    metadata_files = []
    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if re.match("metadata-.*csv.gz", file) is not None:
                metadata_files.append(file)

    metadata_files.sort(reverse=True)
    
    #Get The full path for the latest metadata
    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if file == metadata_files[0]:
                return os.path.join(root, file)



def load_config(filename):
    with open(filename) as config_file:
        config = yaml.load(config_file)
    return config

def setup_logging(filename, console_enabled):
    FORMAT = '%(asctime)-15s - %(levelname)-5s - %(module)-10s - %(message)s'

    log_handlers = []

    file_log_handler = logging.handlers.RotatingFileHandler(filename=filename, maxBytes=10485760, backupCount=5)

    log_handlers.append(file_log_handler)

    if console_enabled:
        console = logging.StreamHandler()
        log_handlers.append(console)

    logging.basicConfig(level=logging.DEBUG, format=FORMAT, handlers=log_handlers)

    # We need a custom level to have 'FATAL' appear in log files (instead of CRITICAL)
    logging.addLevelName(FATAL_ERROR, 'FATAL')


def parsed_line_to_metadata_obj(line):
    coldef = sr.ColumnMetadata()
    coldef.schema =line[0]
    coldef.table = line[1]
    coldef.name = line[2]
    coldef.type = line[3]
    coldef.attnum = line[4]

    return coldef

def read_metadata(filename):

    columns = []
    with gzip.open(filename, 'rt') as metadata_file:
        for line in metadata_file:
            parsed_line = line.strip('\n').split("\013")
            if parsed_line[1] == 'users':
                columns.append(parsed_line_to_metadata_obj(parsed_line))
    # TODO sort by attnum

    return columns

def move_files_between_folders(f_from, f_to, filename_pattern):
    # TODO: only copy 6000 files at a time
    file_move_cnt = 0
    for root, dirs, files in os.walk("./" + f_from):
        for file in files:
            if re.match(filename_pattern, file) is not None:
                src = os.path.join(root, file)
                if f_from == "uploads":
                    trg = os.path.join(root, file).replace("./uploads" + os.path.sep + "public" + os.path.sep, "./" + f_to + os.path.sep)
                else:
                    trg = os.path.join(root, file).replace("./" + f_from + os.path.sep, "./" + f_to + os.path.sep)

                os.makedirs(os.path.dirname(trg), exist_ok = True)
                shutil.move(src, trg)
                file_move_cnt += 1

    logging.debug("{} {} file(s) moved from {} to {}".format(file_move_cnt, filename_pattern, f_from, f_to));


def manage_partitions(db, schema, table):

    if table in ("threadinfo", "serverlogs", "plainlogs"):
        query = ("select {schema_name}.manage_partitions('{schema_name}', '{table_name}')").format(schema_name = schema, table_name = table)
        db.execute_in_transaction(query)


def insert_data_from_external_table(db, schema, metadata, src_table, trg_table):

    query = "INSERT INTO {schema_name}.{trg_table_name} ( \n" + \
            sr.get_columns_def(metadata, schema, src_table, False) + \
            " ) \n" \
            "SELECT \n" + \
                sr.get_columns_def(metadata, schema, src_table, False) + \
            " FROM {schema_name}.ext_{src_table_name}"

    query = query.format(schema_name = schema, src_table_name = src_table, trg_table_name = trg_table)
    result = db.execute_non_query_in_transaction(query)
    logging.info("Populated Table = {}, Inserted = {}".format(trg_table, result))


def load_data(db, metadata, schema, table):

    logging.info("Start loading data from external table - {}".format(table))

    try:
        pass
        # move_files_between_folders("processing", "retry", table +
        # ".*csv.gz")

        #move_files_between_folders("uploads", "processing", table + ".*csv.gz")
        #manage_partitions(db, schema, table)
        #insert_data_from_external_table(db, schema, table, metadata)
        #move_files_between_folders("processing", "archive", table + ".*csv.gz")

    except Exception as exception:
        logging.error(("Loading data for {} faild. Files are moving to retry folder. {}").format(table))
        logging.error(exception)
        #move_files_between_folders("processing", "retry", table + ".*csv.gz")

    logging.info("End loading data from external table - {}".format(table))


def parse_datetime(filename):
    # 'users-2016-09-14--07-48-13--seq0000--part0000-csv-09-14--07-48-f31c477b94cf356270439b096942d10d.csv.gz'
    if re.search('part\d{4}', filename) is not None:
        match = re.search('\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2}', filename)
        date = match.group(0).split('--')
        date = ' '.join([date[0].replace('-','.'), date[1].replace('-',':')])
        return date
    else:
        raise PaletteFileParseError("Error: The filename doesn't contain a partXXXX substring.")


def apply_scd(db, columns_def, schema, table, filename):


    file_date = parse_datetime(filename)
    logging.info("Processing: {} file date: {}".format(table, file_date))
    map = sr.getSQL(columns_def, schema, table, 'yes', ['id'], file_date)
    query = 'truncate table {schema_name}.s_{table_name}'.format(schema_name = schema, table_name = table)
    db.execute_non_query_in_transaction(query)
    insert_data_from_external_table(db, schema, columns_def, 'users', 's_users')

    for q in [map["DWHtableUpdateSCD"], map["DWHtableInsertSCD"]]:
        db.execute_non_query_in_transaction(q)
        sleep(30)

    # db.execute_non_query_in_transaction(map["DWHtableUpdateSCD"])
    # db.execute_non_query_in_transaction(map["DWHtableInsertSCD"])

    # java.sql.Connection
    # conn = (java.sql.Connection)
    # globalMap.get("conn_tPostgresqlConnection_1");
    # globalMap.put("SCDSucceeded", false);
    # if (context.scd.equalsIgnoreCase("yes")) {
    # int numStageInserts = 0;
    # PreparedStatement DWHtableUpdateSCDSQL=null;
    # PreparedStatement DWHtableInsertSCDSQL=null;
    # PreparedStatement loadStage = null;
    # String currentQuery="";
    # try {
    # currentQuery = "truncate table " + context.Target_Schema + ".s_" + context.tableName;
    # conn.createStatement().execute(currentQuery);
    # // currentQuery = "INSERT INTO " + context.Target_Schema + ".s_" + context.tableName + " SELECT "+ +"* FROM " + context.Target_Schema + ".ext_" + context.tableName;
    # currentQuery= "INSERT INTO " + context.Target_Schema + ".s_" + context.tableName + " ( " + ((String)globalMap.get("EXTtableColumns")) + " )\n"+
    # " SELECT " + ((String)globalMap.get("EXTtableColumns")) +" FROM " + context.Target_Schema + ".ext_" + context.tableName;
    #
    # loadStage=conn.prepareStatement(currentQuery);
    # numStageInserts = loadStage.executeUpdate();
    # conn.commit();
    #
    # log.warn("Populated stage - Table=" + context.tableName + " Inserted=" + numStageInserts );
    # talendMeter_METTER.addMessage("Stage Inserted", numStageInserts, "", "", "tFlowLogger_1");
    # talendMeter_METTERProcess(globalMap);
    #
    # conn.setAutoCommit(false);
    # currentQuery=(String)globalMap.get("DWHtableUpdateSCD");
    # DWHtableUpdateSCDSQL=conn.prepareStatement(currentQuery);
    # int numSCDUpdates = DWHtableUpdateSCDSQL.executeUpdate();
    #
    # currentQuery=(String)globalMap.get("DWHtableInsertSCD");
    # DWHtableInsertSCDSQL=conn.prepareStatement(currentQuery);
    # int numSCDInserts = DWHtableInsertSCDSQL.executeUpdate();
    #
    # conn.commit();
    # log.warn("SCD Completed - Table=" + context.tableName +" Updated=" + numSCDUpdates + " Inserted=" + numSCDInserts );
    # talendMeter_METTER.addMessage("SCD Updated", numSCDUpdates, "", "", "tFlowLogger_1");
    # talendMeter_METTERProcess(globalMap);
    # talendMeter_METTER.addMessage("SCD Inserted", numSCDInserts, "", "", "tFlowLogger_1");
    # talendMeter_METTERProcess(globalMap);
    # globalMap.put("SCDSucceeded", true);
    # }
    # catch(SQLException
    # e )
    # {
    #     log.fatal(
    #         "SCD FAILED - Table=" + context.tableName + " error was:" + e.getMessage()
    #         + " Query was '" + currentQuery);
    # talendLogs_LOGS.addMessage("tWarn", "SCD", 5,
    #                            "Error when loading data" + e.getMessage(), 50);
    # talendLogs_LOGSProcess(globalMap);
    # if (conn != null)
    # {
    # try
    #     {
    #         log.info("Transaction is being rolled back");
    #     conn.rollback();
    #     }
    #     catch(SQLException
    #     excep)
    #     {
    #         log.fatal(excep.getMessage());
    #     }
    #     }
    #     globalMap.put("SCDSucceeded", false);
    #     }
    #     finally
    #     {
    #     if (DWHtableUpdateSCDSQL != null)
    #     DWHtableUpdateSCDSQL.close();
    #     if (DWHtableInsertSCDSQL != null) DWHtableInsertSCDSQL.close();
    #     if (loadStage != null) loadStage.close();
    #     conn.setAutoCommit(true);
    #     }
    # }


def main():

    try:
        config_filename = sys.argv[1]
        config = load_config(config_filename)

        setup_logging(config['Logfilename'], config['ConsoleLog'])


        logging.info('Start Insight GP-Import.')

        db = Database(config)

        latest_metadata_file =  get_latest_metadata_file()
        logging.debug("Metadata file: " + latest_metadata_file)

        metadata = read_metadata(latest_metadata_file)

        # cre_dwh_table_query = sr.get_create_table_query(metadata, 'palette', 'threadinfo')
        # create_external_table_query = sr.get_create_external_table_query(metadata, 'palette', 'threadinfo')
        # print(sr.get_table_columns_def_from_db(db, 'palette', 'serverlogs'))
        # print(sr.has_ext_table_structure_changed(db, 'palette', 'threadinfo', metadata))
        # print(sr.if_table_exists(db, 'palette', 'threadinfo'))
        #sr.create_table_if_not_exists(db, 'palette', 'threadinfoo', metadata)

        #todo: handle execption in order not to stop all the table loads beacause of one table's problem
        # load_data(db, metadata, 'palette', 'threadinfo')
        apply_scd(db, metadata, 'palette', 'users', 'users-2016-09-14--07-48-13--seq0000--part0000-csv-09-14--07-48-f31c477b94cf356270439b096942d10d.csv')

        logging.info('End Insight GP-Import.')


    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise


if __name__ == '__main__':
    main()