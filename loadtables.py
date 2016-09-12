import logging
import logging.handlers
import yaml
import sys
import gzip
import sql_routines as sr

FATAL_ERROR = 49

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

def main():
    try:
        config_filename = sys.argv[1]
        config = load_config(config_filename)

        setup_logging(config['Logfilename'], config['ConsoleLog'])

        logging.info('Start Insight GP-Import.')

        metadata = read_metadata("metadata-2016-08-25--09-47-20--seq0000--part0000-csv-08-25--09-47-6b25c7dcb5ce6c32e452c7d32d0b7e7e.csv.gz")

        cre_dwh_table_query = sr.get_create_table_query(metadata, 'palette', 'threadinfo')

        logging.info('End Insight GP-Import.')
    except Exception as exception:
        logging.log(FATAL_ERROR, 'Unhandled exception occurred: {}'.format(exception))
        raise


if __name__ == '__main__':
    main()

