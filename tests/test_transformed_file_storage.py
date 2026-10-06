import tempfile
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from core.utils import sftp_utils
from core.utils.config_utils import Config


class LocalSFTP:
    """Exercise remote copy operations against an isolated filesystem."""

    def stat(self, path):
        return Path(path).stat()

    def mkdir(self, path):
        Path(path).mkdir()

    def open(self, path, mode):
        return open(path, mode)

    def get(self, source, destination):
        shutil.copyfile(source, destination)

    def put(self, source, destination):
        shutil.copyfile(source, destination)


class TransformedFileStorageTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        config_path = self.root / 'config.ini'
        config_path.write_text(
            '[sftp]\n'
            f'transformed_files_location = {self.root}/transformed\n'
            f'transformed_files_backup_location = {self.root}/backup\n'
        )
        self.config = Config(config_path)
        self.source = self.root / 'input.csv'
        self.source.write_bytes(b'column\ntransformed data\n')
        connection = patch.object(sftp_utils, '_open_sftp').start()
        patch.object(sftp_utils, 'get_run_logger').start()
        connection.return_value.__enter__.return_value = LocalSFTP()
        self.addCleanup(patch.stopall)

    def test_stages_exact_mapped_file_and_uploads_local_result(self):
        local = sftp_utils.download_mapped_file_via_sftp(
            str(self.source), self.root / 'staging', self.config,
        )
        self.assertEqual(local, self.root / 'staging/input.csv')
        self.assertEqual(local.read_bytes(), self.source.read_bytes())
        local.write_bytes(b'transformed result\n')
        target, backup = sftp_utils.upload_transformed_file_via_sftp(
            local, 'Example/Partner', self.config,
        )
        self.assertEqual(Path(target), self.root / 'transformed/Example-Partner/input.csv')
        self.assertEqual(Path(backup), self.root / 'backup/input.csv')
        self.assertEqual(Path(target).read_bytes(), local.read_bytes())
        self.assertEqual(Path(backup).read_bytes(), local.read_bytes())
        self.assertEqual(self.source.read_bytes(), b'column\ntransformed data\n')

    def test_local_upload_validates_config_before_transfer(self):
        self.config.parser.remove_option('sftp', 'transformed_files_backup_location')
        with patch.object(sftp_utils, 'put_sftp') as upload:
            with self.assertRaisesRegex(ValueError, 'transformed_files_backup_location'):
                sftp_utils.upload_transformed_file_via_sftp(self.source, 'Partner', self.config)
            upload.assert_not_called()

    def test_local_upload_backup_failure_propagates(self):
        with patch.object(sftp_utils, 'put_sftp', side_effect=[None, OSError('backup failed')]):
            with self.assertRaisesRegex(OSError, 'backup failed'):
                sftp_utils.upload_transformed_file_via_sftp(self.source, 'Partner', self.config)

    def test_download_rejects_directory_paths(self):
        for path in ('', '/mapped/', '/mapped/..'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                sftp_utils.download_mapped_file_via_sftp(path, self.root / 'staging', self.config)

    def test_copies_content_creates_directories_and_retains_source(self):
        target, backup = sftp_utils.copy_transformed_file(
            str(self.source), 'Example/Partner', config=self.config
        )
        self.assertEqual(Path(target), self.root / 'transformed/Example-Partner/input.csv')
        self.assertEqual(Path(backup), self.root / 'backup/input.csv')
        self.assertEqual(Path(target).read_bytes(), self.source.read_bytes())
        self.assertEqual(Path(backup).read_bytes(), self.source.read_bytes())
        # Existing partner folders are reused and both copies are refreshed.
        self.source.write_bytes(b'updated\n')
        sftp_utils.copy_transformed_file(str(self.source), 'Example/Partner', self.config)
        self.assertEqual(Path(target).read_bytes(), b'updated\n')
        self.assertEqual(Path(backup).read_bytes(), b'updated\n')

    def test_missing_source_fails(self):
        with self.assertRaises(FileNotFoundError):
            sftp_utils.copy_transformed_file(str(self.root / 'missing.csv'), 'Partner', self.config)

    def test_missing_backup_setting_fails_before_copy(self):
        self.config.parser.remove_option('sftp', 'transformed_files_backup_location')
        with self.assertRaisesRegex(ValueError, 'transformed_files_backup_location'):
            sftp_utils.copy_transformed_file(str(self.source), 'Partner', self.config)
        self.assertFalse((self.root / 'transformed').exists())

    def test_invalid_partner_folder_fails(self):
        for name in ('', '/', '.', '..'):
            with self.assertRaises(ValueError):
                sftp_utils.copy_transformed_file(str(self.source), name, self.config)

    def test_mapped_upload_returns_path(self):
        with patch.object(sftp_utils, 'save_to_target', return_value='/mapped/input.csv'):
            self.assertEqual(
                sftp_utils.save_to_mapped_via_sftp(
                    self.source, 'input.csv', 'Partner', config=self.config,
                ),
                '/mapped/input.csv',
            )
