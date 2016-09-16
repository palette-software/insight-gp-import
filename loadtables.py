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

FATAL_ERROR = 49


class PaletteFileParseError(Exception):
    pass

class PaletteMultipartSCD(Exception):
    pass

def list_files_from_upload_folder(filename_pattern, sort_order):
    sorted_file_list = []
    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if re.match(filename_pattern + "-.*csv.gz", file) is not None:
                sorted_file_list.append(file)

    sort_order = sort_order.lower()
    if sort_order == "asc":
        sorted_file_list.sort(reverse=False)
    elif sort_order == "desc":
        sorted_file_list.sort(reverse=True)

    return sorted_file_list

def get_latest_metadata_file():

    metadata_files = list_files_from_upload_folder("metadata", "desc")

    #Get The full path for the latest metadata
    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if file == metadata_files[0]:
                return os.path.join(root, file)



def load_config(filename):
    with open(filename) as config_file:
        config = yaml.load(config_file)
    return config

def setup_logging(filename, console_enabled, log_level):
    FORMAT = '%(asctime)-15s - %(levelname)-5s - %(module)-10s - %(message)s'

    log_handlers = []

    file_log_handler = logging.handlers.RotatingFileHandler(filename=filename, maxBytes=10485760, backupCount=5)

    log_handlers.append(file_log_handler)

    if console_enabled:
        console = logging.StreamHandler()
        log_handlers.append(console)

    if log_level == "DEBUG":
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level, format=FORMAT, handlers=log_handlers)

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
            metadata_obj = parsed_line_to_metadata_obj(parsed_line)
            if metadata_obj.type in TYPE_CONVERSION_MAP.keys():
                metadata_obj.type = TYPE_CONVERSION_MAP[metadata_obj.type]
            columns.append(metadata_obj)
    # TODO sort by attnum

    return columns

def move_files_between_folders(f_from, f_to, filename_pattern, full_match = False):

    # TODO: only copy 6000 files at a time if load_type = incremental load
    file_move_cnt = 0
    if not full_match:
        filename_pattern += "-"
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

    logging.debug("{} {} file(s) moved from {} to {}".format(file_move_cnt, filename_pattern, f_from, f_to))


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

def chk_multipart_scd_filenames_in_uploads_folder(table):

    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if re.match(table + ".+part0001.+csv\.gz", file) is not None:
                raise PaletteMultipartSCD("MultiPart SCD table, STOPPING! File = {}".format(file))

def apply_scd(db, columns_def, schema, table, filename):

    file_date = parse_datetime(filename)
    logging.info("Processing: {} file date: {}".format(table, file_date))
    map = sr.getSQL(columns_def, schema, table, 'yes', ['id'], file_date)
    query = 'truncate table {schema_name}.s_{table_name}'.format(schema_name = schema, table_name = table)
    db.execute_non_query_in_transaction(query)
    insert_data_from_external_table(db, schema, columns_def, 'users', 's_users')

    queries_in_transaction = [map["DWHtableUpdateSCD"], map["DWHtableInsertSCD"]]
    upd_ins_rowcount = db.execute_non_query_in_transaction(queries_in_transaction)
    logging.info("SCD Completed. Table = {}, Updated = {}, Inserted = {}".format(table, upd_ins_rowcount[0], upd_ins_rowcount[1]))

    #if success, or not success...

def get_metadata_for_table(metadata, table):
    metadata_for_table = []
    for m in metadata:
        if m.table == table:
            metadata_for_table.append(m)
    return metadata_for_table


def handle_incremental_tables(config, metadata):

    for table in config["Tables"]["Incremental"]:
        metadata_for_table = get_metadata_for_table(metadata, table)
        if table == "threadinfo":
            (sr.get_create_external_table_query(metadata_for_table, config["Schema"], table))
            (sr.get_create_incremental_table_query(metadata_for_table, config["Schema"], table))

def create_external_table_if_needed(db, schema, table, metadata_for_table, gpfdist_addr):

    ext_table = "ext_" + table
    ext_table_create_sql = sr.get_create_external_table_query(metadata_for_table, schema, table)
    ext_table_create_sql = ext_table_create_sql.replace("#EXTERNAL_TABLE","gpfdist://{gpfdist_addr}/*/{table_name}-*.csv.gz".format(gpfdist_addr=gpfdist_addr, table_name=table))

    table_exists = sr.table_exists(db, schema, ext_table)
    if not table_exists:
        db.execute_non_query_in_transaction(ext_table_create_sql)
        logging.info("External table created: {table_name}".format(table_name = ext_table))

    # Check if structure has modifed
    alter_list = sr.gen_alter_cols_because_of_metadata_change(db, schema, table, metadata_for_table, incremental=False)
    gpfdist_addr_modified = sr.ext_table_gpfdist_addr_modified(db, schema, ext_table, gpfdist_addr)

    if alter_list != [] or gpfdist_addr_modified:
        sr.drop_table(db, schema, ext_table, external=True)
        db.execute_non_query_in_transaction(ext_table_create_sql)
        logging.info("External table recreated: {table_name}".format(table_name=ext_table))
    return alter_list

def alter_dwh_table_if_needed(db, alter_list):
    db.execute_non_query_in_transaction(alter_list)

def handle_full_tables(config, metadata, db):

    for item in config["Tables"]["Full"]:
        table = item["name"]
        metadata_for_table = get_metadata_for_table(metadata, table)
        if table == "users":
            logging.info("Start processing table: {}".format(table))
            #Todo: how to improve this? passing alter_list is ugly but we want to avoid double db call
            alter_list = create_external_table_if_needed(db, config["Schema"], table, metadata_for_table, config["gpfdist_addr"])
            alter_dwh_table_if_needed(db, alter_list)

            #in case some files were stuck here from prev. run
            move_files_between_folders("processing", "retry", table)

            #we have to deal with "full table" files one by one in ascending order
            file_list = list_files_from_upload_folder(table, "asc")
            for file in file_list:
                move_files_between_folders("uploads", "processing", file, True)
                scd_date = parse_datetime(file)
                sql_queries_map = sr.getSQL(metadata_for_table, config["Schema"], table, "yes", item["pk"], scd_date)

            logging.info("End processing table: {}".format(table))


TYPE_CONVERSION_MAP = {
    'uuid': 'character varying (166)'
}

def main():
    # TODO:
    # if there is a new Tableau(TM) version, and there are files from the
    # previous version, currently we only process the newest metadata, thus
    # we can't process the older csvs
    try:
        config_filename = sys.argv[1]
        config = load_config(config_filename)

        setup_logging(config['Logfilename'], config['ConsoleLog'], config['LogLevel'])


        logging.info('Start Insight GP-Import.')

        db = Database(config)

        latest_metadata_file =  get_latest_metadata_file()
        logging.debug("Metadata file: " + latest_metadata_file)

        metadata = read_metadata(latest_metadata_file)

        #load_incremental_tables(config, metadata)


        handle_full_tables(config, metadata, db)

        # cre_dwh_table_query = sr.get_create_table_query(metadata, 'palette', 'threadinfo')
        #create_external_table_query = sr.get_create_external_table_query(metadata, 'palette', 'threadinfo')
        # print(sr.get_table_columns_def_from_db(db, 'palette', 'serverlogs'))
        # print(sr.has_ext_table_structure_changed(db, 'palette', 'threadinfo', metadata))
        # print(sr.if_table_exists(db, 'palette', 'threadinfo'))

        #todo: handle execption in order not to stop all the table loads beacause of one table's problem
        # load_data(db, metadata, 'palette', 'threadinfo')
        #apply_scd(db, metadata, 'palette', 'users', 'users-2016-09-14--07-48-13--seq0000--part0000-csv-09-14--07-48-f31c477b94cf356270439b096942d10d.csv')
        #chk_multipart_scd_filenames_in_uploads_folder("users")


        logging.info('End Insight GP-Import.')


    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise


if __name__ == '__main__':
    main()