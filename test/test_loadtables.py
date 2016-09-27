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

    def fake_list_files_from_folder(self, folder_path, filename_pattern, sort_order):
        return [f for f in self.csv_files if f.startswith(filename_pattern)]

    def fake_move_files_between_folders(self, storage_path, f_from, f_to, filename_pattern, full_match=False):
        return len(self.fake_list_files_from_folder(None, filename_pattern, None))

    @patch('loadtables.adjust_table_to_metadata')
    @patch('sql_routines._db')
    @patch('sql_routines.create_dwh_incremantal_tables_if_needed')
    @patch('sql_routines.insert_data_from_external_table')
    @patch('loadtables.move_files_between_folders')
    @patch('loadtables.list_files_from_folder')
    def test_handle_incremental_tables(self, mock_list_files, mock_move_files, mock_insert_data, mock_dwh, mock_db, mock_adjust):
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

        mock_dwh.return_value = False
        mock_list_files.side_effect = self.fake_list_files_from_folder
        mock_move_files.side_effect = self.fake_move_files_between_folders

        loadtables.handle_incremental_tables(config, metadata)

        # Not called for async_jobs (no files) and customized_views (not in config)
        mock_insert_data.assert_called_with(None, 'ext_threadinfo', 'threadinfo')
