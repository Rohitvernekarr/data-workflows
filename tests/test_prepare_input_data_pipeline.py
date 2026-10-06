import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from xml.etree import ElementTree
from zipfile import ZipFile

from core.data_ingestion import prepare_input_data_pipeline as pipeline


def mapping(raw, target, suggested=None, mandatory=True):
    return {
        "expected_raw_file_field": raw,
        "uploaded": raw,
        "suggested": suggested or raw,
        "input_field": "legacy_target",
        "transformed_column_field": target,
        "output_field": "Display header, not a database column",
        "is_mandatory": mandatory,
    }


class ReadXlsxRowsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'input.xlsx'

    def write_workbook(self, dimension=None, skip_rows=0, named_sheet=False):
        import openpyxl

        workbook = openpyxl.Workbook()
        sheet = workbook.active
        if named_sheet:
            sheet.append(['Wrong sheet'])
            sheet = workbook.create_sheet('Data')
        for _ in range(skip_rows):
            sheet.append(['Partner report'])
        sheet.append(['SKU', 'Quantity', 'Date'])
        sheet.append(['00123', 2, datetime(2026, 5, 25)])
        sheet.append(['00124', None, None])
        workbook.save(self.path)
        workbook.close()

        if dimension is not None:
            sheet_path = f'xl/worksheets/sheet{2 if named_sheet else 1}.xml'
            with ZipFile(self.path) as archive:
                entries = [(info, archive.read(info)) for info in archive.infolist()]
            with ZipFile(self.path, 'w') as archive:
                for info, content in entries:
                    if info.filename == sheet_path:
                        root = ElementTree.fromstring(content)
                        namespace = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
                        root.find(f'{namespace}dimension').set('ref', dimension)
                        content = ElementTree.tostring(root)
                    archive.writestr(info, content)

    def test_reads_actual_cells_regardless_of_cached_dimensions(self):
        for dimension in (None, 'A1:A1', 'A1:B2', 'A1:Z100'):
            for skip_rows, named_sheet in ((0, False), (1, True)):
                with self.subTest(dimension=dimension, skip_rows=skip_rows):
                    self.write_workbook(dimension, skip_rows, named_sheet)
                    headers, rows = pipeline._read_rows(
                        self.path, 'Data' if named_sheet else None, skip_rows, None,
                    )
                    self.assertEqual(headers, ('SKU', 'Quantity', 'Date'))
                    self.assertEqual(rows, [
                        ('00123', 2, datetime(2026, 5, 25)),
                        ('00124', None, None),
                    ])

    def test_skip_rows_beyond_sheet_fails(self):
        self.write_workbook()
        with self.assertRaisesRegex(ValueError, 'no header row'):
            pipeline._read_rows(self.path, None, 3, None)


class PrepareRecordsTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(pipeline, 'get_run_logger')
        logger.start()
        self.addCleanup(logger.stop)

    def prepare(self, headers, rows, mappings):
        cfg = Mock()
        with patch.object(pipeline, 'fetch_partner_mappings', return_value=mappings) as fetch:
            result = pipeline.prepare_records(headers, rows, '123_POS', cfg)
            fetch.assert_called_once_with(cfg, '123_POS')
            return result

    def test_api_data_envelope_and_api_column_names(self):
        self.assertEqual(self.prepare(['Resolved SKU'], [['001']], {'data': [{
            'expected_raw_file_field': 'Resolved SKU',
            'transformed_column_field': 'product_code',
        }]}), (['product_code'], [{'product_code': '001'}]))

    def test_invalid_api_responses_fail(self):
        for response in (None, [], {}, {'data': []}, {'data': 'invalid'}, [None]):
            with self.subTest(response=response), self.assertRaises(ValueError):
                self.prepare(['SKU'], [['001']], response)

    def test_legacy_target_is_not_used(self):
        with self.assertRaisesRegex(ValueError, 'No transformed columns match'):
            self.prepare(['SKU'], [['001']], [{
                'uploaded': 'SKU', 'input_field': 'old', 'input_column_field': 'SKU',
            }])

    def test_raw_headers_map_to_db_columns_and_preserve_identifiers(self):
        columns, records = self.prepare(
            ['SKU', 'Quantity', 'Notes'],
            [['00123', '2', 'ignore'], ['00124', '', ''], ['', '', '']],
            [mapping('SKU', 'product_code'), mapping('Quantity', 'quantity')],
        )
        self.assertEqual(columns, ['product_code', 'quantity'])
        self.assertEqual(records, [
            {'product_code': '00123', 'quantity': '2'},
            {'product_code': '00124', 'quantity': None},
        ])

    def test_expected_raw_header_takes_precedence_over_legacy_aliases(self):
        self.assertEqual(self.prepare(
            ['Raw SKU', 'Resolved SKU'], [['001', 'wrong']],
            [mapping('Raw SKU', 'product_code', 'Resolved SKU')],
        )[1], [{'product_code': '001'}])

    def test_missing_expected_raw_field_does_not_fall_back_to_aliases(self):
        for source in (None, '', '   '):
            item = mapping('SKU', 'product_code')
            item['expected_raw_file_field'] = source
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, 'expected_raw_file_field'):
                self.prepare(['SKU'], [['001']], [item])
        with self.assertRaisesRegex(ValueError, 'Missing mandatory'):
            self.prepare(['Resolved SKU'], [['001']], [mapping('Raw SKU', 'code', 'Resolved SKU')])

    def test_missing_mandatory_header_fails(self):
        with self.assertRaisesRegex(ValueError, 'Missing mandatory'):
            self.prepare(['Other'], [['x']], [mapping('SKU', 'product_code')])

    def test_expected_raw_header_matches_case_insensitively(self):
        self.assertEqual(self.prepare([' sku '], [['AbC001']], [{
            'expected_raw_file_field': ' SKU ', 'transformed_column_field': 'ProductCode',
        }]), (['ProductCode'], [{'ProductCode': 'AbC001'}]))

    def test_one_source_can_populate_multiple_targets(self):
        self.assertEqual(self.prepare(['SKU'], [['001']], [
            mapping('SKU', 'code'), mapping('SKU', 'original_code'),
        ]), (['code', 'original_code'], [{'code': '001', 'original_code': '001'}]))

    def test_duplicate_unmapped_headers_do_not_block_ingestion(self):
        self.assertEqual(self.prepare(['SKU', 'Note', 'note'], [['001', 'a', 'b']], [
            mapping('SKU', 'code'),
        ]), (['code'], [{'code': '001'}]))

    def test_columns_without_targets_are_omitted(self):
        mappings = [mapping('SKU', 'product_code'), mapping('Optional', '', mandatory=False),
                    {'source': 'ignored', 'uploaded': 'Ignored'}]
        self.assertEqual(self.prepare(['SKU', 'Ignored'], [['1', 'x']], mappings)[1],
                         [{'product_code': '1'}])

    def test_populated_targets_require_headers_regardless_of_mandatory_flag(self):
        for flag in (True, False, None, 'false'):
            with self.subTest(flag=flag), self.assertRaisesRegex(ValueError, 'Missing mandatory'):
                self.prepare(['SKU'], [['1']], [
                    mapping('SKU', 'product_code'), mapping('Missing', 'quantity', mandatory=flag),
                ])
        with self.assertRaisesRegex(ValueError, 'Missing mandatory'):
            self.prepare(['SKU'], [['1']], [{
                'expected_raw_file_field': 'Missing', 'transformed_column_field': 'quantity',
            }])

    def test_populated_target_is_mapped_even_when_source_is_ignored(self):
        item = {**mapping('SKU', 'product_code', mandatory=False), 'source': 'ignored'}
        self.assertEqual(self.prepare(['SKU'], [['001']], [item]),
                         (['product_code'], [{'product_code': '001'}]))

    def test_missing_db_target_does_not_use_display_header(self):
        with self.assertRaisesRegex(ValueError, 'No transformed columns match'):
            self.prepare(['SKU'], [['1']], [{'uploaded': 'SKU', 'suggested': 'SKU'}])

    def test_unpopulated_targets_are_skipped_even_when_mandatory(self):
        mappings = [mapping('SKU', 'product_code')]
        for target in (None, '', '   '):
            mappings.append(mapping('Missing mandatory header', target))
        mappings.append({'uploaded': 'Notes', 'is_mandatory': True})
        self.assertEqual(
            self.prepare(['SKU', 'Notes'], [['001', 'ignore']], mappings),
            (['product_code'], [{'product_code': '001'}]),
        )

    def test_duplicate_targets_and_ambiguous_headers_fail(self):
        cases = [
            (['SKU', 'Other'], [mapping('SKU', 'code'), mapping('Other', 'code')]),
            (['SKU', 'SKU'], [mapping('SKU', 'code')]),
            (['SKU', ' sku '], [mapping('SKU', 'code')]),
        ]
        for headers, mappings in cases:
            with self.subTest(headers=headers), self.assertRaises(ValueError):
                self.prepare(headers, [], mappings)

    def test_extra_values_fail_and_short_rows_use_null(self):
        mappings = [mapping('SKU', 'code'), mapping('Quantity', 'quantity')]
        with self.assertRaisesRegex(ValueError, 'more values'):
            self.prepare(['SKU', 'Quantity'], [['1', '2', '3']], mappings)
        self.assertEqual(self.prepare(['SKU', 'Quantity'], [['1']], mappings)[1],
                         [{'code': '1', 'quantity': None}])


class IngestionPipelineTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(pipeline, 'get_run_logger')
        logger.start()
        self.addCleanup(logger.stop)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.config = Path(directory.name) / 'config.ini'
        self.config.write_text('[sftp]\ntransformed_files_location = /transformed\n')
        self.payload = {
            'partner_id': '123_POS', 'file_name': 'input.csv',
            'transformed_file_path': '/transformed/Partner/input.csv',
            'skip_rows': 1,
        }
        self.fetch = patch.object(pipeline, 'fetch_partner_mappings', return_value=[
            mapping('SKU', 'product_code'), mapping('Quantity', 'quantity'),
        ]).start()
        self.download = patch.object(pipeline, 'get_sftp').start()
        self.addCleanup(patch.stopall)
        self.download.side_effect = lambda remote, local, config: local.write_text(
            'Partner report\nSKU,Quantity,Note\n00123,2,"note, with comma"\n00124,,\n'
        )

    def test_downloads_exact_file_maps_records_and_cleans_staging(self):
        result = pipeline.execute(self.payload, self.config)
        self.assertEqual(result['row_count'], 2)
        self.assertEqual(result['input_records'][0], {'product_code': '00123', 'quantity': '2'})
        self.assertIsNone(result['input_records'][1]['quantity'])
        self.assertEqual(result['partner_id'], '123_POS')
        self.assertEqual(self.fetch.call_args.args[1], '123_POS')
        self.assertEqual(self.fetch.call_args.args[0].path, self.config)
        self.fetch.assert_called_once()
        remote, local = self.download.call_args.args
        self.assertEqual(remote, '/transformed/Partner/input.csv')
        self.assertFalse(local.exists())

    def test_tsv_and_custom_delimiter(self):
        for filename, delimiter in [('input.tsv', '\t'), ('input.csv', ';')]:
            payload = {**self.payload, 'file_name': filename, 'skip_rows': 0,
                       'transformed_file_path': f'/transformed/Partner/{filename}'}
            if delimiter == ';':
                payload['delimiter'] = delimiter
            self.download.side_effect = lambda remote, local, config: local.write_text(
                f'SKU{delimiter}Quantity\n00123{delimiter}2\n'
            )
            self.assertEqual(pipeline.execute(payload, self.config)['row_count'], 1)

    def test_none_skips_download(self):
        self.assertIsNone(pipeline.execute(None, self.config))
        self.download.assert_not_called()

    def test_missing_partner_and_path_mismatch_fail_before_download(self):
        for update in [
            {'partner_id': None}, {'partner_id': '  '}, {'file_name': 'other.csv'},
            {'transformed_file_path': '/elsewhere/input.csv'}, {'skip_rows': -1},
        ]:
            with self.subTest(update=update), self.assertRaises(ValueError):
                pipeline.execute({**self.payload, **update}, self.config)
        self.download.assert_not_called()

    def test_download_failure_propagates(self):
        self.download.side_effect = FileNotFoundError('transformed file missing')
        with self.assertRaises(FileNotFoundError):
            pipeline.execute(self.payload, self.config)

    def test_staging_is_cleaned_on_mapping_failure(self):
        self.fetch.return_value = [mapping('Missing', 'missing')]
        with self.assertRaises(ValueError):
            pipeline.execute(self.payload, self.config)
        self.assertFalse(self.download.call_args.args[1].exists())

    def test_stale_payload_mappings_are_ignored(self):
        self.payload['partner_mappings'] = [mapping('Missing', 'stale_target')]
        result = pipeline.execute(self.payload, self.config)
        self.assertEqual(result['input_columns'], ['product_code', 'quantity'])

    def test_api_failure_propagates_and_cleans_staging(self):
        self.fetch.side_effect = TimeoutError('mapping API unavailable')
        with self.assertRaises(TimeoutError):
            pipeline.execute(self.payload, self.config)
        self.assertFalse(self.download.call_args.args[1].exists())
