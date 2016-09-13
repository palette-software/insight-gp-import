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
            if parsed_line[1] == 'threadinfo':
                columns.append(parsed_line_to_metadata_obj(parsed_line))

    return columns

def move_files_between_folders(f_from, f_to, filename_pattern):

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



def load_data(db, metadata, schema, table):

    logging.info("Start loading data from external table - {}".format(table))

    move_files_between_folders("uploads", "processing", table + ".*csv.gz")

    query = "INSERT INTO {schema_name}.{table_name} ( " + \
            sr.get_columns_def(metadata, schema, table, False) + \
            " ) " \
            "SELECT " + sr.get_columns_def(metadata, schema, table, False) + \
            " FROM {schema_name}.ext_{table_name}"

    query = query.format(schema_name = schema, table_name = table)
    db.execute_in_transaction(query, None)

    logging.info("End loading data from external table - {}".format(table))


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
        # sr.create_table_if_not_exists(db, 'palette', 'threadinfo', metadata)

        load_data(db, metadata, 'palette', 'threadinfo')

        logging.info('End Insight GP-Import.')

    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise


if __name__ == '__main__':
    main()