import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.data_transformation import transform_input_data_pipeline as pipeline
from core.utils.partner_config_utils import PartnerConfig


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config = self.root / 'config.ini'
        self.config.write_text('[bigquery]\nproject = test\n')
        self.partner = PartnerConfig()
        self.partner.partner_name = 'Example Partner'
        self.logger = patch.object(pipeline, 'get_run_logger').start().return_value
        self.copy = patch.object(pipeline, 'upload_transformed_file_via_sftp').start()
        self.copy.side_effect = lambda local, partner, config: (
            f'/transformed/{partner}/{local.name}', f'/backup/{local.name}',
        )
        self.download = patch.object(pipeline, 'download_mapped_file_via_sftp').start()
        self.staged = []
        def download(remote, directory, config):
            local = Path(directory) / Path(remote).name
            local.write_text('SKU,Quantity\n00123,2\n')
            self.staged.append(local)
            return local
        self.download.side_effect = download
        self.lookup = patch.object(pipeline, 'fetch_partner_config', return_value=self.partner).start()
        self.addCleanup(patch.stopall)
        self.payload = {'partner_id': '123_POS', 'mapped_file_path': '/mapped/input.csv'}

    def test_no_resolved_input_skips_lookup(self):
        self.assertIsNone(pipeline.execute(None))
        self.lookup.assert_not_called()
        self.copy.assert_not_called()

    def test_not_required_copies_for_ingestion(self):
        with patch.object(pipeline.subprocess, 'run') as run:
            result = pipeline.execute(self.payload, self.config)
            self.assertEqual(result["file_name"], "input.csv")
            self.assertEqual(result["transformed_file_path"], "/transformed/Example Partner/input.csv")
            run.assert_not_called()
        self.copy.assert_called_once()
        self.assertFalse(self.staged[0].parent.exists())
        self.assertEqual(self.lookup.call_args.args[1], '123_POS')
        self.assertEqual(self.lookup.call_args.kwargs, {'strict': True})

    def test_relative_script_runs_with_context(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = 'preprocess.py'
        (self.root / 'preprocess.py').write_text(
            'import os\nfrom pathlib import Path\n'
            'Path(__file__).with_suffix(".result").write_text('
            'os.environ["PARTNER_ID"] + "|" + os.environ["MAPPED_FILE_PATH"])\n'
            'Path(os.environ["LOCAL_TRANSFORMED_FILE_PATH"]).write_bytes(Path(os.environ["LOCAL_MAPPED_FILE_PATH"]).read_bytes())\n'
        )
        result = pipeline.execute(self.payload, self.config)
        self.assertEqual(result["transformed_file_path"],
                         "/transformed/Example Partner/input_Transformed.xlsx")
        self.assertEqual(result["transformed_file_backup_path"], "/backup/input_Transformed.xlsx")
        local_output, partner = self.copy.call_args.args
        self.assertEqual(local_output.name, 'input_Transformed.xlsx')
        self.assertEqual(partner, 'Example Partner')
        self.assertFalse(local_output.parent.exists())
        self.assertEqual(result['skip_rows'], 0)
        self.assertIsNone(result['sheet_name'])
        self.assertEqual((self.root / 'preprocess.result').read_text(), '123_POS|/mapped/input.csv')

    def test_required_script_missing(self):
        self.partner.preprocessing_required = True
        with self.assertRaises(ValueError):
            pipeline.execute(self.payload, self.config)
        self.partner.preprocessing_script_path = 'missing.py'
        with self.assertRaises(FileNotFoundError):
            pipeline.execute(self.payload, self.config)

    def test_script_failure_propagates(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'fail.py')
        (self.root / 'fail.py').write_text('raise SystemExit(7)\n')
        with self.assertRaises(subprocess.CalledProcessError) as error:
            pipeline.execute(self.payload, self.config)
        self.assertEqual(error.exception.returncode, 7)
        self.copy.assert_not_called()
        self.assertFalse(self.staged[0].parent.exists())

    def test_copy_failure_propagates(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'success.py')
        (self.root / 'success.py').write_text('import os\nfrom pathlib import Path\nPath(os.environ["LOCAL_TRANSFORMED_FILE_PATH"]).write_text("result")\n')
        self.copy.side_effect = OSError('backup failed')
        with self.assertRaisesRegex(OSError, 'backup failed'):
            pipeline.execute(self.payload, self.config)
        self.assertFalse(self.staged[0].parent.exists())

    def test_script_inherits_runtime_environment_and_working_directory(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'runtime.py')
        (self.root / 'runtime.py').write_text(
            'import json, os, sys\nfrom pathlib import Path\n'
            'Path(__file__).with_suffix(".json").write_text(json.dumps({\n'
            '"executable": sys.executable, "prefix": sys.prefix, "cwd": os.getcwd(),\n'
            '"inherited": os.environ["PREPROCESS_TEST_VALUE"]}))\n'
            'Path(os.environ["LOCAL_TRANSFORMED_FILE_PATH"]).write_text("result")\n'
            'print("processing complete")\n'
            'print("script warning", file=sys.stderr)\n'
        )
        with patch.dict(os.environ, {'PREPROCESS_TEST_VALUE': 'inherited value'}):
            original_environment = dict(os.environ)
            pipeline.execute(self.payload, self.config)
            self.assertEqual(dict(os.environ), original_environment)
        runtime = json.loads((self.root / 'runtime.json').read_text())
        self.assertEqual(runtime, {
            'executable': sys.executable, 'prefix': sys.prefix, 'cwd': os.getcwd(),
            'inherited': 'inherited value',
        })
        self.logger.info.assert_any_call('Preprocessing stdout:\n%s', 'processing complete')
        self.logger.warning.assert_called_once_with('Preprocessing stderr:\n%s', 'script warning')

    def test_script_traceback_is_logged_and_failure_propagates(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'error.py')
        (self.root / 'error.py').write_text('print("processing started")\nraise ValueError("bad input")\n')
        with self.assertRaises(subprocess.CalledProcessError) as error:
            pipeline.execute(self.payload, self.config)
        self.assertIn('ValueError: bad input', error.exception.stderr)
        self.logger.error.assert_any_call(
            'Preprocessing stderr:\n%s', error.exception.stderr.rstrip()
        )
        self.logger.error.assert_any_call(
            'Preprocessing failed for partner %s: %s (exit code %s)',
            '123_POS', (self.root / 'error.py').resolve(), error.exception.returncode,
        )
        self.logger.info.assert_any_call('Preprocessing stdout:\n%s', 'processing started')
        self.copy.assert_not_called()

    def test_process_launch_error_is_logged(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'launch.py')
        (self.root / 'launch.py').write_text('pass\n')
        with patch.object(pipeline.subprocess, 'run', side_effect=OSError('launch failed')):
            with self.assertRaisesRegex(OSError, 'launch failed'):
                pipeline.execute(self.payload, self.config)
        self.logger.exception.assert_called_once()
        self.copy.assert_not_called()

    def test_missing_output_fails_without_upload_and_cleans_staging(self):
        self.partner.preprocessing_required = True
        self.partner.preprocessing_script_path = str(self.root / 'empty.py')
        (self.root / 'empty.py').write_text('pass\n')
        with self.assertRaisesRegex(FileNotFoundError, 'did not create output'):
            pipeline.execute(self.payload, self.config)
        self.copy.assert_not_called()
        self.assertFalse(self.staged[0].parent.exists())

    def test_download_failure_prevents_script_and_upload(self):
        self.download.side_effect = OSError('download failed')
        with patch.object(pipeline.subprocess, 'run') as run:
            with self.assertRaisesRegex(OSError, 'download failed'):
                pipeline.execute(self.payload, self.config)
            run.assert_not_called()
        self.copy.assert_not_called()

    def test_lookup_failure_propagates(self):
        self.lookup.side_effect = RuntimeError('lookup failed')
        with self.assertRaisesRegex(RuntimeError, 'lookup failed'):
            pipeline.execute(self.payload, self.config)

    def test_invalid_partner_id(self):
        for payload in ({}, {'partner_id': ''}, {'partner_id': None}):
            with self.assertRaises(ValueError):
                pipeline.execute(payload, self.config)
        self.lookup.assert_not_called()


if __name__ == '__main__':
    unittest.main()
