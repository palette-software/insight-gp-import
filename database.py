import logging
import psycopg2
from time import sleep

class Database(object):
    def __init__(self, kwargs):
        logging.debug("Start creating database connection")
        self.connection = psycopg2.connect(
            "dbname={Database} user={User} password={Password} host={Host} port={Port}".format(**kwargs))
        logging.debug("End creating database connection")
        self.schema_name = kwargs['Schema']

    def execute_in_transaction(self, query, params = None):
        logging.debug("query: " + query)
        logging.debug("query parameters: " + str(params))
        with self.connection as connection:
            with connection.cursor() as cursor:
                result = self.__execute(cursor, query, params)
        return result

    def execute_non_query_in_transaction(self, query, params = None):
        logging.debug("query: " + query)
        logging.debug("query parameters: " + str(params))
        with self.connection as connection:
            with connection.cursor() as cursor:
                result = self.__execute_non_query(cursor, query, params)
        return result

    def __execute_non_query(self, cursor, query, params = None):
        cursor.execute(query, params)
        return cursor.rowcount

    def __execute(self, cursor, query, params):
        #logging.debug('Start "{}"'.format(query))
        cursor.execute(query, params)

        if self.__has_no_records(cursor):
            #logging.debug('End "{}"'.format(query))
            return []

        records = cursor.fetchall()
        #logging.debug('End "{}" return with: {}'.format(query, records[0][0]))
        return records

    def __has_no_records(self, cursor):
        return cursor.rowcount < 1 or cursor.statusmessage.startswith('DELETE')

    def __del__(self):
        self.connection.close()
        logging.debug("Database connection is closed")

