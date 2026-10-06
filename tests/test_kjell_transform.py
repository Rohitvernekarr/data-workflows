import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from verisure.transform_scripts import kjell
from core.data_transformation import transform_input_data_pipeline as pipeline
from core.utils import sftp_utils


class KjellTransformTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.config = self.root / "config.ini"
        self.config.write_text("[sftp]\ntransformed_files_location = /custom/transformed\n")
        self.input = pd.DataFrame([{
            "ItemNo": 123, "Description": "Camera", "Description2": "Arlo",
            "Model": " ABC ", "OemItemNo": None, "VendorItemNo": "V123",
            "SalesStoreSE": 1, "SalesWebSE": 2, "InventorySE": 3,
            "SalesStoreNO": 4, "SalesWebNO": 5, "SalesAVCTotal": 6,
            "InventoryNO": 7, "SalesTotal": 18, "InventoryTotal": 10,
        }])

    def test_local_transformation_preserves_business_rules(self):
        for extension, separator in ((".csv", ","), (".CSV", ";"),
                                     (".xlsx", None), (".txt", "\t")):
            with self.subTest(extension=extension):
                source = self.root / f"report_20260524_2026_05_25{extension}"
                if separator is None:
                    self.input.to_excel(source, index=False)
                else:
                    self.input.to_csv(source, sep=separator, index=False)
                result = kjell.main(source)
                self.assertEqual(result, source.with_name(
                    "report_20260524_2026_05_25_Transformed.xlsx"))
                output = pd.read_excel(result)
                self.assertEqual(len(output), 7)
                self.assertEqual(output.Sales.dropna().tolist(), [1, 2, 4, 5, 6])
                self.assertEqual(output.Inventory.dropna().tolist(), [3, 7])
                self.assertEqual(output.SoldToName.tolist(), [
                    "Kjell SE", "Kjell SE", "Kjell SE", "Kjell NO",
                    "Kjell NO", "Kjell DK", "Kjell NO",
                ])
                self.assertTrue(output.OemItemNo.eq("ABC").all())
                self.assertTrue(output.DATE.eq("05/25/2026").all())
                self.assertTrue(source.exists())

    def test_explicit_output_path(self):
        source = self.root / 'input.csv'
        self.input.to_csv(source, index=False)
        output = self.root / 'result.xlsx'
        self.assertEqual(kjell.main(source, output), output)
        self.assertEqual(len(pd.read_excel(output)), 7)

    def test_pipeline_uploads_generated_workbook_and_cleans_staging(self):
        self.config.write_text(
            '[sftp]\ntransformed_files_location = /custom/transformed\n'
            'transformed_files_backup_location = /custom/backup\n'
        )
        staged = []
        uploaded = []
        def download(remote, local, config):
            self.assertEqual(remote, '/mapped/KJELL/report_20260525.csv')
            staged.append(Path(local))
            self.input.to_csv(local, index=False)
        def upload(local, remote, config):
            staged.append(Path(local))
            uploaded.append((pd.read_excel(local), remote))
        partner = SimpleNamespace(
            partner_name='KJELL (ARLO)', preprocessing_required=True,
            preprocessing_script_path=str(Path(kjell.__file__).resolve()),
        )
        with patch.object(pipeline, 'fetch_partner_config', return_value=partner), \
             patch.object(pipeline, 'get_run_logger'), \
             patch.object(sftp_utils, 'get_run_logger'), \
             patch.object(sftp_utils, 'get_sftp', side_effect=download), \
             patch.object(sftp_utils, 'put_sftp', side_effect=upload):
            result = pipeline.execute({
                'partner_id': '123_POS',
                'mapped_file_path': '/mapped/KJELL/report_20260525.csv',
                'sheet_name': 'Old sheet', 'skip_rows': 3,
            }, self.config)
        self.assertEqual(result['file_name'], 'report_20260525_Transformed.xlsx')
        self.assertEqual(result['transformed_file_path'],
                         '/custom/transformed/KJELL (ARLO)/report_20260525_Transformed.xlsx')
        self.assertEqual(result['transformed_file_backup_path'],
                         '/custom/backup/report_20260525_Transformed.xlsx')
        self.assertEqual(len(uploaded), 2)
        self.assertEqual(len(uploaded[0][0]), 7)
        self.assertTrue(uploaded[0][0].DATE.eq('05/25/2026').all())
        self.assertTrue(all(not path.parent.exists() for path in staged))
        self.assertEqual(result['skip_rows'], 0)
        self.assertIsNone(result['sheet_name'])

    def test_invalid_input_and_same_output_fail(self):
        with self.assertRaises(ValueError):
            kjell.main(self.root / 'input.pdf')
        with self.assertRaises(FileNotFoundError):
            kjell.main(self.root / 'missing.csv')
        with self.assertRaisesRegex(ValueError, 'must differ'):
            kjell.main(self.root / 'input.xlsx', self.root / 'input.xlsx')
