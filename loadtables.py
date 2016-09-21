import logging
import logging.handlers
import yaml
import sys
import gzip
import sql_routines
from database import Database
import os
import re
import shutil

FATAL_ERROR = 49
VERTICAL_TAB = "\013"

class PaletteFileParseError(Exception):
    pass


class PaletteMultipartSCD(Exception):
    pass

# TODO roadsByLength = sorted(roads, key=lambda x: x['length'], reverse=False)
def list_files_from_folder(folder_name, filename_pattern, sort_order):
    sorted_file_list = []
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if re.match(filename_pattern + "-.*csv.gz", file) is not None:
                sorted_file_list.append(file)

    sort_order = sort_order.lower()
    if sort_order == "asc":
        sorted_file_list.sort(reverse=False)
    elif sort_order == "desc":
        sorted_file_list.sort(reverse=True)

    return sorted_file_list

#TODO rewrite after list_files_from_folder rewrite
def get_latest_metadata_file(storage_path):
    uploads_path = os.path.join(storage_path, 'uploads')
    metadata_files = list_files_from_folder(uploads_path, "metadata", "desc")

    # Get The full path for the latest metadata
    for root, dirs, files in os.walk(uploads_path):
        for file in files:
            if file == metadata_files[0]:
                return os.path.join(root, file)

    return None


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


def parsed_line_to_metadata_dict(line):
    coldef = {}

    coldef["schema"] = line[0]
    coldef["table"] = line[1]
    coldef["name"] = line[2]
    coldef["type"] = line[3]
    coldef["attnum"] = line[4]
    coldef["length"] = 0
    coldef["precision"] = 0

    return coldef

# TODO consider to use python csv loader
def read_metadata(filename):
    ret = {}
    with gzip.open(filename, 'rt') as metadata_file:
        header_line = next(metadata_file)
        for line in metadata_file:
            parsed_line = line.strip('\n').split(VERTICAL_TAB)
            metadata_dict = parsed_line_to_metadata_dict(parsed_line)
            if metadata_dict['table'] not in ret:
                ret[metadata_dict['table']] = []
            if metadata_dict["type"] in TYPE_CONVERSION_MAP.keys():
                metadata_dict["type"] = TYPE_CONVERSION_MAP[metadata_dict["type"]]
            ret[metadata_dict['table']].append(metadata_dict)

    for key, value in ret.items():
        ret[key] = sorted(value, key=lambda x: int(x["attnum"]))

    return ret


def move_files_between_folders(storage_path, f_from, f_to, filename_pattern, full_match=False):
    # TODO only copy 6000 files at a time if load_type = incremental load
    from_path = os.path.join(storage_path, f_from)
    file_move_cnt = 0
    if not full_match:
        filename_pattern += "-"
    for root, dirs, files in os.walk(from_path):
        for file in files:
            if re.match(filename_pattern, file) is not None:
                src = os.path.join(root, file)
                trg = get_target_path(f_from, f_to, src)

                os.makedirs(os.path.dirname(trg), exist_ok=True)
                shutil.move(src, trg)
                file_move_cnt += 1

    logging.debug("{} {} file(s) moved from {} to {}".format(file_move_cnt, filename_pattern, f_from, f_to))
    return file_move_cnt


def get_target_path(f_from, f_to, src):
    if f_from == "uploads":
        trg = src.replace("uploads" + os.path.sep + "public", f_to)
    else:
        trg = src.replace(f_from + os.path.sep, f_to + os.path.sep)
    return trg


def parse_datetime(filename):
    # 'users-2016-09-14--07-48-13--seq0000--part0000-csv-09-14--07-48-f31c477b94cf356270439b096942d10d.csv.gz'
    if re.search('part\d{4}', filename) is not None:
        match = re.search('\d{4}-\d{2}-\d{2}--\d{2}-\d{2}-\d{2}', filename)
        date = match.group(0).split('--')
        date = ' '.join([date[0].replace('-', '.'), date[1].replace('-', ':')])
        return date
    else:
        raise PaletteFileParseError("Error: The filename doesn't contain a partXXXX substring.")


def chk_multipart_scd_filenames_in_uploads_folder(table):
    for root, dirs, files in os.walk("./uploads"):
        for file in files:
            if re.match(table + ". + part0000.+csv\.gz", file) is None:
                raise PaletteMultipartSCD("MultiPart SCD table, STOPPING! File = {}".format(file))

def processing_retry_folder(storage_path, table, metadata_for_table):
    file_list = list_files_from_folder("retry", table, "asc")
    if len(file_list) == 0:
        return

    logging.info("Start processing retry folder for table: {}".format(table))
    for file in file_list:
        try:
            logging.info("Start processing file: {}".format(file))
            move_files_between_folders(storage_path, "retry", "processing", file, True)
            sql_routines.insert_data_from_external_table(metadata_for_table, "ext_" + table, table)
            move_files_between_folders(storage_path, "processing", "archive", table)
            logging.info("End processing file: {}".format(file))
        except Exception as e:
            logging.error(
                "Incremental Load RETRY failed: {}. File moved to retried folder and will not be processed further. Exception: {}".format(
                    file, e))
            move_files_between_folders(storage_path, "processing", "retried", file, True)

    logging.info("End processing retry folder for table: {}".format(table))


def handle_incremental_tables(config, metadata):
    logging.info("Start loading incremental tables.")

    data_path = config["storage_path"]
    for table in config["Tables"]["Incremental"]:
        try:

            logging.info("Start processing table: {}".format(table))

            metadata_for_table = metadata[table]
            sql_routines.manage_partitions(table)
            processing_retry_folder(config["storage_path"], table, metadata_for_table)
            if sql_routines.create_dwh_incremantal_tables_if_needed(table, metadata_for_table):
                logging.info("Table created: {}".format(table))
                continue

            adjust_table_to_metadata(config["gpfdist_addr"], True, metadata_for_table, table)
            move_files_between_folders(data_path, "uploads", "processing", table)
            sql_routines.insert_data_from_external_table(metadata_for_table, "ext_" + table, table)
            move_files_between_folders(data_path, "processing", "archive", table)

            logging.info("End processing table: {}".format(table))

        except Exception as e:
            logging.error("Processing failed for: {}. Exception: {}".format(table, e))
            move_files_between_folders(data_path, "processing", "retry", table)

    logging.info("End loading incremental tables.")


def handle_full_tables(config, metadata):
    logging.info("Start loading full tables.")

    schema = config["Schema"]
    data_path = config["storage_path"]

    for item in config["Tables"]["Full"]:

        try:
            table = item["name"]

            logging.info("Start processing table: {}".format(table))
            metadata_for_table = metadata[table]
            sql_queries_map = sql_routines.getSQL(metadata_for_table, table, "yes", item["pk"], None)
            chk_multipart_scd_filenames_in_uploads_folder(table)

            if sql_routines.create_dwh_full_tables_if_needed(table, sql_queries_map):
                logging.info("Table created: {}".format(table))
                continue

            adjust_table_to_metadata(config["gpfdist_addr"], False, metadata_for_table, table)

            # in case some files were stuck here from prev. run
            move_files_between_folders(data_path, "processing", "retry", table)

            # we have to deal with "full table" files one by one in ascending order
            file_list = list_files_from_folder("uploads", table, "asc")
            for file in file_list:
                try:
                    move_files_between_folders(data_path, "uploads", "processing", file, True)
                    sql_routines.load_data_from_external_table(metadata_for_table, table)
                    scd_date = parse_datetime(file)
                    sql_routines.apply_scd(metadata_for_table, table, scd_date, item["pk"])
                    move_files_between_folders(data_path, "processing", "archive", table)
                except Exception as e:
                    logging.error(
                        "SCD processing failed for {}. File moved to retry folder and will not be processed further. Exception: {}".format(
                            file, e))
                    move_files_between_folders(data_path, "processing", "retry", file, True)
            logging.info("End processing table: {}".format(table))
        except Exception as e:
            logging.error("Processing failed for: {}. Exception: {}".format(table, e))

    logging.info("End loading full tables.")


def adjust_table_to_metadata(gpfdist_addr, incremental, metadata_for_table, table):
    sql_routines.recreate_external_table(table, metadata_for_table, gpfdist_addr,
                                         incremental)
    # Check if structure has modifed
    alter_list = sql_routines.gen_alter_cols_because_of_metadata_change(table, metadata_for_table, incremental)
    sql_routines.alter_dwh_table_if_needed(alter_list)


TYPE_CONVERSION_MAP = {
    #There is no uuid type in Postgres 8.2 (Greenplum)
    'uuid': 'character varying (166)'
}


def main():
    # TODO
    # if there is a new Tableau(TM) version, and there are files from the
    # previous version, currently we only process the newest metadata, thus
    # we can't process the older csvs

    # TODO handle execption in order not to stop all the table loads beacause of one table's problem

    try:
        config_filename = sys.argv[1]
        storage_path = sys.argv[2]
        config = load_config(config_filename)
        config['storage_path'] = storage_path

        setup_logging(config['Logfilename'], config['ConsoleLog'], config['LogLevel'])

        logging.info('Start Insight GP-Import.')
        db = Database(config)
        sql_routines.init(db, config["Schema"])

        latest_metadata_file = get_latest_metadata_file(config['storage_path'])
        logging.debug("Metadata file: " + latest_metadata_file)
        metadata = read_metadata(latest_metadata_file)

        handle_incremental_tables(config, metadata)
        handle_full_tables(config, metadata)

        logging.info('End Insight GP-Import.')


    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise


if __name__ == '__main__':
    main()
