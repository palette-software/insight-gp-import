from unittest import TestCase
from unittest.mock import patch
import loadtables


class TestLoadtables(TestCase):
    def setUp(self):
        super().setUp()
        self.csv_files = [
            'threadinfo-2016-09-26--13-50-12--seq0000--part0000-csv-09-26--13-50-3219b4d8f83259b5565b18884a070b90.csv.gz',
            'threadinfo-2016-09-26--14-17-43--seq0000--part0000-csv-09-26--14-17-7096246f722ae41d1eb8763d0c916064.csv.gz',
            'threadinfo-2016-09-26--14-42-44--seq0000--part0000-csv-09-26--14-42-b520a115ff9cebef5000ed0bdc4d82c5.csv.gz',
            'customized_views-2016-09-26--19-25-38--seq0000--part0000-csv-09-26--19-25-2fcebfb5db9d5da71e7be685433f3f98.csv.gz'
        ]

        self.metadata_from_csv_for_users = [
            {'name': 'id', 'precision': 0, 'type': 'integer', 'length': 0, 'table': 'users', 'schema': 'public',
             'attnum': '1'},
            {'name': 'login_at', 'precision': 0, 'type': 'timestamp without time zone', 'length': 0, 'table': 'users',
             'schema': 'public', 'attnum': '2'},
            {'name': 'licensing_role_id', 'precision': 0, 'type': 'integer', 'length': 0, 'table': 'users',
             'schema': 'public', 'attnum': '3'}]

        self.metadata_from_db_for_users = [
            {'name': 'id', 'precision': 0, 'type': 'integer', 'length': 0, 'table': 'h_users', 'schema': 'palette',
             'attnum': 1},
            {'name': 'licensing_role_id', 'precision': 0, 'type': 'integer', 'length': 0, 'table': 'h_users',
             'schema': 'palette', 'attnum': 3},
            {'name': 'nonce', 'precision': 0, 'type': 'character varying(32)', 'length': 0, 'table': 'h_users',
             'schema': 'palette', 'attnum': 4}]

    def fake_list_files_from_folder(self, folder_path, filename_pattern, sort_order):
        return [f for f in self.csv_files if f.startswith(filename_pattern)]

    def fake_move_files_between_folders(self, storage_path, f_from, f_to, filename_pattern, full_match=False):
        return len(self.fake_list_files_from_folder(None, filename_pattern, None))

    @patch('loadtables.get_common_metadata')
    @patch('loadtables.get_metadata_from_db')
    @patch('loadtables.adjust_table_to_metadata')
    @patch('sql_routines.SqlRoutines')
    @patch('loadtables.move_files_between_folders')
    @patch('loadtables.list_files_from_folder')
    def test_handle_incremental_tables(self, mock_list_files, mock_move_files, mock_sql_routines, mock_adjust, mock_get_metadata_from_db, mock_common):
        config = {
            "storage_path": "/fake",
            "Tables": {
                "Incremental": ['threadinfo', 'async_jobs']
            },
            'gpfdist_addr': None
        }

        metadata = {
            'threadinfo': None,
            'async_jobs': None,
        }

        mock_sql_routines.create_dwh_incremantal_tables_if_needed.return_value = False
        mock_list_files.side_effect = self.fake_list_files_from_folder
        mock_move_files.side_effect = self.fake_move_files_between_folders
        mock_get_metadata_from_db.return_value = None
        mock_common.return_value = None, None

        loadtables.handle_incremental_tables(config, metadata, mock_sql_routines)

        # Not called for async_jobs (no files) and customized_views (not in config)
        mock_sql_routines.insert_data_from_external_table.assert_called_with(None, 'ext_threadinfo', 'threadinfo')

    @patch('loadtables.adjust_table_to_metadata')
    @patch('loadtables.get_metadata_from_db')
    @patch('sql_routines.SqlRoutines')
    def test_handle_full_tables(self, mock_sql_routines, mock_meta_from_db, mock_adjust):
        config = {
            'Schema': None,
            "storage_path": "/fake",
            'gpfdist_addr': None,
            "Tables": {
                "Full": [
                    {
                        'name': 'users',
                        'pk': None
                    }
                ]
            }
        }
        metadata = {
            'users': self.metadata_from_csv_for_users
        }
        mock_meta_from_db.return_value=self.metadata_from_csv_for_users
        loadtables.handle_full_tables(config, metadata, mock_sql_routines)

    def test_common_metadata_size(self):
        result_metadata, result_error = loadtables.get_common_metadata(self.metadata_from_db_for_users,
                                                                       self.metadata_from_csv_for_users)

        self.assertEqual(2, len(result_metadata))
        self.assertEqual(1, len(result_error))

    def test_common_metadata_item_names_are_in_both_metadata(self):
        result_metadata, result_error = loadtables.get_common_metadata(self.metadata_from_db_for_users,
                                                                       self.metadata_from_csv_for_users)

        result_column_names = set([item["name"] for item in result_metadata])
        result_error_column_names = set([item["name"] for item in result_error])
        self.assertSetEqual(set(('id', 'licensing_role_id')), result_column_names)
        self.assertSetEqual(set(('login_at',)), result_error_column_names)

    def test_common_metadata_not_only_same_table(self):
        metadata_oldusers = [
            {'attnum': '1', 'precision': 0, 'schema': 'public', 'name': 'id', 'type': 'integer', 'length': 0,
             'table': 'oldusers'},
            {'attnum': '2', 'precision': 0, 'schema': 'public', 'name': 'login_at',
             'type': 'timestamp without time zone', 'length': 0, 'table': 'oldusers'},
            {'attnum': '3', 'precision': 0, 'schema': 'public', 'name': 'licensing_role_id', 'type': 'integer',
             'length': 0, 'table': 'oldusers'}]

        result_metadata, result_error = loadtables.get_common_metadata(metadata_oldusers,
                                                                       self.metadata_from_csv_for_users)

        self.assertEqual(3, len(result_metadata))
        self.assertEqual(0, len(result_error))

    def test_common_metadata_column_types_do_not_matter(self):
        metadata_oldusers = [
            {'attnum': '1', 'precision': 0, 'schema': 'public', 'name': 'id', 'type': 'integer', 'length': 0,
             'table': 'users'},
            {'attnum': '2', 'precision': 0, 'schema': 'public', 'name': 'login_at',
             'type': 'timestamp without time zone', 'length': 0, 'table': 'users'},
            # It is double
            {'attnum': '3', 'precision': 0, 'schema': 'public', 'name': 'licensing_role_id', 'type': 'double precision',
             'length': 0, 'table': 'users'}]

        result_metadata, result_error = loadtables.get_common_metadata(metadata_oldusers,
                                                                       self.metadata_from_csv_for_users)

        self.assertEqual(3, len(result_metadata))
        self.assertEqual(0, len(result_error))

    def test_common_metadata_empty_metadata_in_db(self):
        result_metadata, result_error = loadtables.get_common_metadata([],
                                                                       self.metadata_from_csv_for_users)
        self.assertEqual(0, len(result_metadata))
        self.assertEqual(3, len(result_error))

    def test_common_metadata_empty_metadata_in_csv(self):
        result_metadata, result_error = loadtables.get_common_metadata(self.metadata_from_db_for_users,
                                                                       [])
        self.assertEqual(0, len(result_metadata))
        self.assertEqual(0, len(result_error))

    def test_common_metadata_output_type_matches_input_type(self):
        metadata_clone = list(self.metadata_from_db_for_users)
        result_metadata, result_error = loadtables.get_common_metadata(self.metadata_from_db_for_users,
                                                                       metadata_clone)

        self.assertEqual(0, len(result_error))
        self.assertListEqual(result_metadata, metadata_clone)
