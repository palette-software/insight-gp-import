from unittest import TestCase
import sql_routines as sr
import loadtables as lt
import re
import gzip


class TestSqlRoutines(TestCase):

    def test_h_users_gen_insert(self):
        metadata_for_users = [
            {'attnum': '1', 'precision': 0, 'schema': 'public', 'name': 'id', 'type': 'integer', 'length': 0,
             'table': 'users'}, {'attnum': '2', 'precision': 0, 'schema': 'public', 'name': 'login_at',
                                 'type': 'timestamp without time zone', 'length': 0, 'table': 'users'},
            {'attnum': '3', 'precision': 0, 'schema': 'public', 'name': 'licensing_role_id', 'type': 'integer',
             'length': 0, 'table': 'users'},
            {'attnum': '4', 'precision': 0, 'schema': 'public', 'name': 'nonce', 'type': 'character varying(32)',
             'length': 0, 'table': 'users'},
            {'attnum': '5', 'precision': 0, 'schema': 'public', 'name': 'row_limit', 'type': 'integer', 'length': 0,
             'table': 'users'},
            {'attnum': '6', 'precision': 0, 'schema': 'public', 'name': 'storage_limit', 'type': 'integer', 'length': 0,
             'table': 'users'}, {'attnum': '7', 'precision': 0, 'schema': 'public', 'name': 'created_at',
                                 'type': 'timestamp without time zone', 'length': 0, 'table': 'users'},
            {'attnum': '8', 'precision': 0, 'schema': 'public', 'name': 'extracts_required', 'type': 'boolean',
             'length': 0, 'table': 'users'}, {'attnum': '9', 'precision': 0, 'schema': 'public', 'name': 'updated_at',
                                              'type': 'timestamp without time zone', 'length': 0, 'table': 'users'},
            {'attnum': '10', 'precision': 0, 'schema': 'public', 'name': 'admin_level', 'type': 'integer', 'length': 0,
             'table': 'users'},
            {'attnum': '11', 'precision': 0, 'schema': 'public', 'name': 'publisher_tristate', 'type': 'integer',
             'length': 0, 'table': 'users'},
            {'attnum': '12', 'precision': 0, 'schema': 'public', 'name': 'raw_data_suppressor_tristate',
             'type': 'integer', 'length': 0, 'table': 'users'},
            {'attnum': '13', 'precision': 0, 'schema': 'public', 'name': 'site_id', 'type': 'integer', 'length': 0,
             'table': 'users'},
            {'attnum': '14', 'precision': 0, 'schema': 'public', 'name': 'system_user_id', 'type': 'integer',
             'length': 0, 'table': 'users'},
            {'attnum': '15', 'precision': 0, 'schema': 'public', 'name': 'system_admin_auto', 'type': 'boolean',
             'length': 0, 'table': 'users'},
            {'attnum': '16', 'precision': 0, 'schema': 'public', 'name': 'luid', 'type': 'character varying (166)',
             'length': 0, 'table': 'users'},
            {'attnum': '17', 'precision': 0, 'schema': 'public', 'name': 'lock_version', 'type': 'integer', 'length': 0,
             'table': 'users'}]

        result_sql = """
                        INSERT INTO palette.h_users

                                                (

                                                  id,
                  login_at,
                  licensing_role_id,
                  nonce,
                  row_limit,
                  storage_limit,
                  created_at,
                  extracts_required,
                  updated_at,
                  admin_level,
                  publisher_tristate,
                  raw_data_suppressor_tristate,
                  site_id,
                  system_user_id,
                  system_admin_auto,
                  luid,
                  lock_version,
                  p_filepath,
                  p_cre_date,

                                                p_active_flag,

                                                p_valid_from,

                                                p_valid_to

                                              )

                                              SELECT

                                                  t.id,
                  t.login_at,
                  t.licensing_role_id,
                  t.nonce,
                  t.row_limit,
                  t.storage_limit,
                  t.created_at,
                  t.extracts_required,
                  t.updated_at,
                  t.admin_level,
                  t.publisher_tristate,
                  t.raw_data_suppressor_tristate,
                  t.site_id,
                  t.system_user_id,
                  t.system_admin_auto,
                  t.luid,
                  t.lock_version,
                  t.p_filepath,
                  t.p_cre_date,

                                                'Y'::VARCHAR(1) p_active_flag,

                                                 CASE WHEN t.sql_type='INSERT' THEN coalesce(t.created_at,'10010101'::date::timestamp) ELSE '2016-06-08 06:12:22'::TIMESTAMP END p_valid_from ,

                                                '21000101'::DATE::TIMESTAMP p_valid_to

                                              FROM

                                              (

                                                SELECT

                                                  CASE

                                  WHEN h_users.id IS NULL THEN 'INSERT'

                                  WHEN s_users.id IS NULL THEN 'DELETE'

                                  WHEN h_users.id IS NOT NULL AND s_users.id IS NOT NULL AND h_users.id=s_users.id THEN 'UPDATE'

                                  ELSE 'N/A'

                                END sql_type,


                    CASE

                                  WHEN s_users.login_at IS NULL AND h_users.login_at IS NULL THEN 'Y'

                                  WHEN s_users.login_at IS NOT NULL AND h_users.login_at IS NOT NULL AND s_users.login_at=h_users.login_at THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.licensing_role_id IS NULL AND h_users.licensing_role_id IS NULL THEN 'Y'

                                  WHEN s_users.licensing_role_id IS NOT NULL AND h_users.licensing_role_id IS NOT NULL AND s_users.licensing_role_id=h_users.licensing_role_id THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.nonce IS NULL AND h_users.nonce IS NULL THEN 'Y'

                                  WHEN s_users.nonce IS NOT NULL AND h_users.nonce IS NOT NULL AND s_users.nonce=h_users.nonce THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.row_limit IS NULL AND h_users.row_limit IS NULL THEN 'Y'

                                  WHEN s_users.row_limit IS NOT NULL AND h_users.row_limit IS NOT NULL AND s_users.row_limit=h_users.row_limit THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.storage_limit IS NULL AND h_users.storage_limit IS NULL THEN 'Y'

                                  WHEN s_users.storage_limit IS NOT NULL AND h_users.storage_limit IS NOT NULL AND s_users.storage_limit=h_users.storage_limit THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.created_at IS NULL AND h_users.created_at IS NULL THEN 'Y'

                                  WHEN s_users.created_at IS NOT NULL AND h_users.created_at IS NOT NULL AND s_users.created_at=h_users.created_at THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.extracts_required IS NULL AND h_users.extracts_required IS NULL THEN 'Y'

                                  WHEN s_users.extracts_required IS NOT NULL AND h_users.extracts_required IS NOT NULL AND s_users.extracts_required=h_users.extracts_required THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.updated_at IS NULL AND h_users.updated_at IS NULL THEN 'Y'

                                  WHEN s_users.updated_at IS NOT NULL AND h_users.updated_at IS NOT NULL AND s_users.updated_at=h_users.updated_at THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.admin_level IS NULL AND h_users.admin_level IS NULL THEN 'Y'

                                  WHEN s_users.admin_level IS NOT NULL AND h_users.admin_level IS NOT NULL AND s_users.admin_level=h_users.admin_level THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.publisher_tristate IS NULL AND h_users.publisher_tristate IS NULL THEN 'Y'

                                  WHEN s_users.publisher_tristate IS NOT NULL AND h_users.publisher_tristate IS NOT NULL AND s_users.publisher_tristate=h_users.publisher_tristate THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.raw_data_suppressor_tristate IS NULL AND h_users.raw_data_suppressor_tristate IS NULL THEN 'Y'

                                  WHEN s_users.raw_data_suppressor_tristate IS NOT NULL AND h_users.raw_data_suppressor_tristate IS NOT NULL AND s_users.raw_data_suppressor_tristate=h_users.raw_data_suppressor_tristate THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.site_id IS NULL AND h_users.site_id IS NULL THEN 'Y'

                                  WHEN s_users.site_id IS NOT NULL AND h_users.site_id IS NOT NULL AND s_users.site_id=h_users.site_id THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.system_user_id IS NULL AND h_users.system_user_id IS NULL THEN 'Y'

                                  WHEN s_users.system_user_id IS NOT NULL AND h_users.system_user_id IS NOT NULL AND s_users.system_user_id=h_users.system_user_id THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.system_admin_auto IS NULL AND h_users.system_admin_auto IS NULL THEN 'Y'

                                  WHEN s_users.system_admin_auto IS NOT NULL AND h_users.system_admin_auto IS NOT NULL AND s_users.system_admin_auto=h_users.system_admin_auto THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.luid IS NULL AND h_users.luid IS NULL THEN 'Y'

                                  WHEN s_users.luid IS NOT NULL AND h_users.luid IS NOT NULL AND s_users.luid=h_users.luid THEN 'Y'

                                  ELSE 'N'

                                END  ||
                    CASE

                                  WHEN s_users.lock_version IS NULL AND h_users.lock_version IS NULL THEN 'Y'

                                  WHEN s_users.lock_version IS NOT NULL AND h_users.lock_version IS NOT NULL AND s_users.lock_version=h_users.lock_version THEN 'Y'

                                  ELSE 'N'

                                END  ||
                '' is_equal,

                                                    s_users.id,
                  s_users.login_at,
                  s_users.licensing_role_id,
                  s_users.nonce,
                  s_users.row_limit,
                  s_users.storage_limit,
                  s_users.created_at,
                  s_users.extracts_required,
                  s_users.updated_at,
                  s_users.admin_level,
                  s_users.publisher_tristate,
                  s_users.raw_data_suppressor_tristate,
                  s_users.site_id,
                  s_users.system_user_id,
                  s_users.system_admin_auto,
                  s_users.luid,
                  s_users.lock_version,
                  s_users.p_filepath,
                  s_users.p_cre_date,

                                                  h_users.p_valid_from,

                                                  h_users.p_valid_to,

                                                  h_users.p_active_flag

                                                FROM

                                                  (

                                                    select * from (

                                                      select * ,row_number() OVER (PARTITION BY id ORDER BY p_valid_from DESC) as p_rn

                                                      from palette.h_users ) tmp_h_users

                                                      WHERE p_rn = 1


                                                  ) h_users

                                                  FULL OUTER JOIN palette.s_users

                                                  ON h_users.id=s_users.id

                                              ) t

                                              WHERE

                                                t.sql_type='INSERT' OR

                                                (t.sql_type='UPDATE' AND is_equal LIKE '%N%')
                """

        result_sql = re.sub("[\s+]", "", result_sql)
        sr.init(None, "palette")
        sql_def_map = sr.getSQL(metadata_for_users, "users", "yes", ["id"], "2016-06-08 06:12:22")
        users_insert = sql_def_map["DWHtableInsertSCD"]
        users_insert = re.sub("[\s+]", "", users_insert)

        self.assertTrue(result_sql == users_insert)


    def test_threadinfo_gen_insert(self):
        metadata_for_threadinfo = [{'type': 'text', 'name': 'host_name', 'precision': 0, 'attnum': '1', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'text', 'name': 'process', 'precision': 0, 'attnum': '2', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'timestamp without time zone', 'name': 'ts', 'precision': 0, 'attnum': '3', 'schema': 'public',
                                  'length': 0, 'table': 'threadinfo'},
                                 {'type': 'bigint', 'name': 'pid', 'precision': 0, 'attnum': '4', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'bigint', 'name': 'tid', 'precision': 0, 'attnum': '5', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'bigint', 'name': 'cpu_time', 'precision': 0, 'attnum': '6', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'timestamp without time zone', 'name': 'poll_cycle_ts', 'precision': 0, 'attnum': '7',
                                  'schema': 'public', 'length': 0, 'table': 'threadinfo'},
                                 {'type': 'timestamp without time zone', 'name': 'start_ts', 'precision': 0, 'attnum': '8', 'schema': 'public',
                                  'length': 0, 'table': 'threadinfo'},
                                 {'type': 'integer', 'name': 'thread_count', 'precision': 0, 'attnum': '9', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'bigint', 'name': 'working_set', 'precision': 0, 'attnum': '10', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'},
                                 {'type': 'boolean', 'name': 'thread_level', 'precision': 0, 'attnum': '11', 'schema': 'public', 'length': 0,
                                  'table': 'threadinfo'}]

        result_sql = """
                     INSERT INTO palette.threadinfo (
                     "p_filepath"
                     , "host_name"
                     , "process"
                     , "ts"
                     , "pid"
                     , "tid"
                     , "cpu_time"
                     , "poll_cycle_ts"
                     , "start_ts"
                     , "thread_count"
                     , "working_set"
                     , "thread_level"
                     , "p_cre_date"
                     )
                    SELECT
                     "p_filepath"
                     , "host_name"
                     , "process"
                     , "ts"
                     , "pid"
                     , "tid"
                     , "cpu_time"
                     , "poll_cycle_ts"
                     , "start_ts"
                     , "thread_count"
                     , "working_set"
                     , "thread_level"
                     , "p_cre_date"
                     FROM palette.ext_threadinfo
                        """

        result_sql = re.sub("[\s+]", "", result_sql)
        sr.init(None, "palette")
        threadinfo_insert = sr.get_insert_data_from_external_table_query(metadata_for_threadinfo, "ext_threadinfo", "threadinfo")
        threadinfo_insert= re.sub("[\s+]", "", threadinfo_insert)
        self.assertTrue(result_sql == threadinfo_insert)


    def test_metadata_sort(self):

        filename = 'test_metadata_sort.gz'
        file_data = '''publicthreadinfohost_nametext1
publicthreadinfoprocesstext2
publicthreadinfopidbigint4
publicthreadinfotidbigint5
publicthreadinfocpu_timebigint6
publicthreadinfotstimestamp without time zone3
publicthreadinfopoll_cycle_tstimestamp without time zone7
publicthreadinfothread_countinteger9
publicthreadinfoworking_setbigint10
publicthreadinfothread_levelboolean11
publicthreadinfostart_tstimestamp without time zone8'''

        with open(filename, 'wb') as gz:
            data = bytes(file_data, 'utf-8')
            s_out = gzip.compress(data)
            gz.write(s_out)

        trg_metadata_for_threadinfo = [
            {'type': 'text', 'name': 'host_name', 'precision': 0, 'attnum': '1', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'text', 'name': 'process', 'precision': 0, 'attnum': '2', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'timestamp without time zone', 'name': 'ts', 'precision': 0, 'attnum': '3', 'schema': 'public',
             'length': 0, 'table': 'threadinfo'},
            {'type': 'bigint', 'name': 'pid', 'precision': 0, 'attnum': '4', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'bigint', 'name': 'tid', 'precision': 0, 'attnum': '5', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'bigint', 'name': 'cpu_time', 'precision': 0, 'attnum': '6', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'timestamp without time zone', 'name': 'poll_cycle_ts', 'precision': 0, 'attnum': '7',
             'schema': 'public', 'length': 0, 'table': 'threadinfo'},
            {'type': 'timestamp without time zone', 'name': 'start_ts', 'precision': 0, 'attnum': '8', 'schema': 'public',
             'length': 0, 'table': 'threadinfo'},
            {'type': 'integer', 'name': 'thread_count', 'precision': 0, 'attnum': '9', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'bigint', 'name': 'working_set', 'precision': 0, 'attnum': '10', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'},
            {'type': 'boolean', 'name': 'thread_level', 'precision': 0, 'attnum': '11', 'schema': 'public', 'length': 0,
             'table': 'threadinfo'}]

        self.assertTrue(lt.read_metadata(filename) == trg_metadata_for_threadinfo)