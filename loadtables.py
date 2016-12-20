import logging
import logging.handlers
import yaml
import sys
import gzip
from sql_routines import SqlRoutines
from database import Database
import os
import re
import shutil

FATAL_ERROR = 49
VERTICAL_TAB = "\013"
METADATA_INSTALL_PATH = os.path.join("_install", "metadata-install.csv.gz")


class PaletteFileParseError(Exception):
    pass


class PaletteMultipartSCD(Exception):
    pass


# TODO roadsByLength = sorted(roads, key=lambda x: x['length'], reverse=False)
def list_files_from_folder(folder_path, filename_pattern, sort_order):
    sorted_file_list = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if re.match(filename_pattern + "-.*csv.gz", file) is not None:
                sorted_file_list.append(file)

    sort_order = sort_order.lower()
    if sort_order == "asc":
        sorted_file_list.sort(reverse=False)
    elif sort_order == "desc":
        sorted_file_list.sort(reverse=True)

    return sorted_file_list


def get_common_metadata(metadata_db, metadata_csv):
    """
    The DB can contain more columns than the CSV files (older Tableau version). The function returns the columns
    need to be loaded.
    Only column definitions for the same table are expected

    :param metadata_db: Metadata from DB column definitions
    :param metadata_csv: Metadata from Tableau metadata CSV files
    :return: a tuple of common column definitions and column definitions missing from the DB
    """

    if len(metadata_csv) == 0 or len(metadata_db) == 0:
        return [], list(metadata_csv)

    column_name_set_csv = set([column['name'] for column in metadata_csv])
    column_name_set_db = set([column['name'] for column in metadata_db])

    common_columns = column_name_set_csv.intersection(column_name_set_db)
    missing_columns = column_name_set_csv.difference(column_name_set_db)

    result = [item for item in metadata_csv if item['name'] in common_columns]
    missing_from_db = [item for item in metadata_csv if item['name'] in missing_columns]

    return result, missing_from_db


# TODO rewrite after list_files_from_folder rewrite
def get_latest_metadata_file(storage_path):
    uploads_path = os.path.join(storage_path, 'uploads')
    metadata_files = list_files_from_folder(uploads_path, "metadata-\d\d\d\d-\d\d-\d\d", "desc")

    if len(metadata_files) > 0:
        # Get The full path for the latest metadata
        for root, dirs, files in os.walk(uploads_path):
            for file in files:
                if file == metadata_files[0]:
                    return os.path.join(root, file)

    return os.path.join(uploads_path, METADATA_INSTALL_PATH)


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


def get_metadata_from_db(table, sql_routines):
    metadata_from_table = []
    column_def = sql_routines.get_table_columns_def_from_db(table)
    technical_cols = ['p_id', 'p_filepath', 'p_cre_date', 'p_active_flag', 'p_valid_from', 'p_valid_to']
    attnum_i = 1
    for schemaname, tablename, columnname, format_type, attnum in column_def:
        column = {'schema': schemaname,
                  'table': tablename,
                  'name': columnname,
                  'type': format_type,
                  'attnum': attnum_i,
                  'length': 0,
                  'precision': 0}
        if columnname not in technical_cols:
            metadata_from_table.append(column)
            attnum_i += 1
    return metadata_from_table


def move_files_between_folders(storage_path, f_from, f_to, filename_pattern, full_match=False):
    def is_limit_reached(limit):
        return limit >= 6000

    from_path = os.path.join(storage_path, f_from)
    file_move_cnt = 0

    if not full_match:
        filename_pattern += "-"
    for root, dirs, files in os.walk(from_path):
        for file in sorted(files):
            if re.match(filename_pattern, file) is not None:
                src = os.path.join(root, file)
                trg = get_target_path(f_from, f_to, src)

                os.makedirs(os.path.dirname(trg), exist_ok=True)
                shutil.move(src, trg)
                file_move_cnt += 1

            # For the inner loop
            if is_limit_reached(file_move_cnt):
                break
        # For the outer loop
        if is_limit_reached(file_move_cnt):
            break

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


def processing_retry_folder(storage_path, table, metadata_for_table, sql_routines):
    retry_path = os.path.join(storage_path, "retry")
    file_list = list_files_from_folder(retry_path, table, "asc")
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


def handle_incremental_tables(config, metadata_from_csv, sql_routines):
    logging.info("Start loading incremental tables.")

    data_path = config["storage_path"]
    for table in config["Tables"]["Incremental"]:
        try:

            logging.debug("Start processing table: {}".format(table))

            metadata_from_csv_for_table = metadata_from_csv[table]
            processing_retry_folder(data_path, table, metadata_from_csv_for_table, sql_routines)

            adjust_table_to_metadata(config["gpfdist_addr"], True, metadata_from_csv_for_table, table, sql_routines)
            if move_files_between_folders(data_path, "uploads", "processing", table) > 0:
                sql_routines.manage_partitions(table)
                sql_routines.insert_data_from_external_table(metadata_from_csv_for_table, "ext_" + table, table)
                move_files_between_folders(data_path, "processing", "archive", table)
            else:
                logging.debug("No file found for {}".format(table))

            logging.debug("End processing table: {}".format(table))

        except Exception as e:
            logging.error("Processing failed for: {}. Exception: {}".format(table, e))
            move_files_between_folders(data_path, "processing", "retry", table)

    logging.info("End loading incremental tables.")


def handle_full_tables(config, metadata_from_csv, sql_routines):
    logging.info("Start loading full tables.")

    schema = config["Schema"]
    data_path = config["storage_path"]

    for item in config["Tables"]["Full"]:

        try:
            table = item["name"]

            logging.debug("Start processing table: {}".format(table))
            metadata_from_csv_for_table = metadata_from_csv[table]

            metadata_from_db_for_table = get_metadata_from_db("h_" + table, sql_routines)

            logging.debug("Table '{}' metadata from Tableau: {}".format(table, metadata_from_csv_for_table))
            logging.debug("Table '{}' metadata from DB: {}".format(table, metadata_from_db_for_table))

            common_metadata_for_table, common_metadata_error = get_common_metadata(metadata_from_db_for_table,
                                                                                   metadata_from_csv_for_table)

            logging.debug("Table '{}' common metadata: {}".format(table, common_metadata_for_table))
            logging.debug("Table '{}' common metadata errors: {}".format(table, common_metadata_error))

            for errors in common_metadata_error:
                logging.warning(
                    "The column '{}' in table '{}' has no matching counterpart in DB".format(errors['name'],
                                                                                             errors['table']))

            sql_routines.getSQL(common_metadata_for_table, table, "yes", item["pk"], None)
            chk_multipart_scd_filenames_in_uploads_folder(table)

            adjust_table_to_metadata(config["gpfdist_addr"], False, common_metadata_for_table, table, sql_routines)

            # in case some files were stuck here from prev. run
            move_files_between_folders(data_path, "processing", "retry", table)

            # we have to deal with "full table" files one by one in ascending order
            upload_path = os.path.join(config['storage_path'], "uploads")
            file_list = list_files_from_folder(upload_path, table, "asc")
            for file in file_list:
                try:
                    move_files_between_folders(data_path, "uploads", "processing", file, True)
                    sql_routines.load_data_from_external_table(common_metadata_for_table, table)
                    scd_date = parse_datetime(file)

                    sql_routines.apply_scd(common_metadata_for_table, table, scd_date, item["pk"])
                    move_files_between_folders(data_path, "processing", "archive", table)
                except Exception as e:
                    logging.error(
                        "SCD processing failed for {}. File moved to retry folder and will not be processed further. Exception: {}".format(
                            file, e))
                    move_files_between_folders(data_path, "processing", "retry", file, True)

            if len(file_list) == 0:
                logging.debug("No file found for {}".format(table))

            logging.debug("End processing table: {}".format(table))
        except Exception as e:
            logging.error("Processing failed for: {}. Exception: {}".format(table, e))

    logging.info("End loading full tables.")


def adjust_table_to_metadata(gpfdist_addr, incremental, metadata_for_table, table, sql_routines):
    sql_routines.recreate_external_table(table, metadata_for_table, gpfdist_addr,
                                         incremental)


TYPE_CONVERSION_MAP = {
    # There is no uuid type in Postgres 8.2 (Greenplum)
    'uuid': 'character varying (166)'
}


def main():
    # TODO
    # if there is a new Tableau(TM) version, and there are files from the
    # previous version, currently we only process the newest metadata, thus
    # we can't process the older csvs

    if len(sys.argv) != 3:
        print("Usage: {} <config.yml> <storage_path>".format(sys.argv[0]))
        return 1

    try:
        config_filename = sys.argv[1]
        storage_path = sys.argv[2]
        config = load_config(config_filename)
        config['storage_path'] = storage_path

        setup_logging(config['Logfilename'], config['ConsoleLog'], config['LogLevel'])

        logging.info('Start Insight GP-Import Version=%s', config['Version'])
        db = Database(config)
        sr = SqlRoutines(db, config["Schema"])

        latest_metadata_file = get_latest_metadata_file(config['storage_path'])
        logging.debug("Metadata file: " + latest_metadata_file)
        metadata = read_metadata(latest_metadata_file)

        handle_incremental_tables(config, metadata, sr)
        handle_full_tables(config, metadata, sr)

        logging.info('End Insight GP-Import.')


    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise

    finally:
        db.close_connection()

    return 0


if __name__ == '__main__':
    sys.exit(main())
