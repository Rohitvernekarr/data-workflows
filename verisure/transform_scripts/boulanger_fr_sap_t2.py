import pandas as pd
import os
import sys
import re
from pathlib import Path
from get_run_logger import get_run_logger

parentPath = str(Path(__file__).resolve().parent.parent.parent)
sys.path.insert(0, parentPath)
from utils.helper import build_input_file_path, build_output_file_path

PARTNER_LABEL = "BOULANGER FR SAP T-2"
input_folder_path = build_input_file_path("BOULANGER FR SAP T-2")
output_folder_path = build_output_file_path("BOULANGER FR SAP T-2")
sheetname_POS = "Boulanger _ Ventes semaine 21_2"
sheetname_INV = "Boulanger _ Stocks semaine 21_2"


def _normalise_header(header):
    """Compare report headers without being affected by tabs/newlines/spaces."""
    return re.sub(r'\s+', ' ', str(header)).strip().casefold()


def _join_path(*parts):
    """Join filesystem paths while preserving platform-native separators."""
    return os.path.join(*[str(part) for part in parts])


def _normalise_signed_value(value):
    """Normalize signed values from the source files while preserving empty cells.

    Behavior:
    - If value is missing/NaN/empty -> return pd.NA
    - Strip whitespace and NBSP
    - Convert trailing minus notation "5-" -> "-5" (string form). Numeric parsing is done later.
    """
    # Keep missing values as pandas NA so downstream numeric conversion can preserve nulls
    if pd.isna(value):
        return pd.NA
    # Remove whitespace including NBSP
    text = "".join(str(value).split()).replace('\u00A0', '')
    if text == '':
        return pd.NA
    # Convert trailing minus notation like "5-" to "-5"
    if text.endswith('-'):
        return f"-{text[:-1]}"
    return text


def _apply_signed_values_to_frame(df, pattern, target_column):
    """
    Find the first source column whose name matches `pattern` (regex),
    normalise its values (vectorised) and assign them to `target_column`.
    This preserves row alignment and avoids concatenating values from multiple columns.
    """
    # Find the first matching column
    source_column = next(
        (c for c in df.columns if re.search(pattern, str(c), re.I)),
        None,
    )
    if source_column is None:
        return df

    # Vectorised normalization using the helper; preserve index and nulls
    normalized = df[source_column].map(_normalise_signed_value)
    df[target_column] = normalized
    return df


def _find_source_column(dataframe, accepted_headers):
    accepted = {_normalise_header(header) for header in accepted_headers}
    return next(
        (column for column in dataframe.columns if _normalise_header(column) in accepted),
        None,
    )


def _build_pos_layout(df):
    """Apply the updated Boulanger PoS business mapping."""
    mapping = {
        'ID pdt fournisseur': ('Ref Art Fournisseur', 'ID pdt fournisseur'),
        'Dénomination produit': ('Désignation', 'Dénomination produit', 'DÃ©signation', 'DÃ©nomination produit'),
        'Sales': ('Quantité vendue', 'Sales', 'QuantitÃ© vendue', 'QuantitAc vendue', 'QuantitAŸAc vendue'),
        'EAN': ('Code EAN', 'EAN'),
        'Date': ('Date',),
    }
    selected = {}
    for target, aliases in mapping.items():
        source_column = _find_source_column(df, aliases)
        if source_column is None:
            selected[target] = pd.Series([pd.NA] * len(df), index=df.index, dtype='object')
        else:
            selected[target] = df[source_column]

    output = pd.DataFrame(selected)

    # Parse Sales values into numeric types when possible.
    # Handle trailing '-' notation meaning negative value (e.g. "5-" -> -5).
    def _parse_sales_series(series):
        def _parse_val(v):
            if pd.isna(v):
                return pd.NA
            # If already numeric, preserve it
            if isinstance(v, (int, float)):
                return v
            s = str(v).strip()
            if s == '':
                return pd.NA
            # Leading minus from normalization: "-5" or "-5.0"
            neg = False
            if s.startswith('-'):
                neg = True
                s = s[1:]
            # Remove spaces/NBSP and convert commas to dots
            s = s.replace(' ', '').replace('\u00A0', '').replace(',', '.')
            try:
                num = float(s)
                if num.is_integer():
                    num = int(num)
                return -num if neg else num
            except Exception:
                return pd.NA

        parsed = series.map(_parse_val)
        non_null = parsed.dropna()
        # If all non-null values are ints, use nullable Int64, otherwise Float64
        if len(non_null) > 0 and non_null.map(lambda x: isinstance(x, int)).all():
            return pd.Series(parsed, index=series.index, dtype='Int64')
        else:
            return pd.Series(parsed, index=series.index, dtype='Float64')

    output['Sales'] = _parse_sales_series(output['Sales'])
    output['Stock'] = 0

    direct_site_column = _find_source_column(df, ('Site',))
    reported_site_header = next(
        (
            column for column in df.columns
            if re.fullmatch(r'\s*Site\s*\(\s*F?(\d+)\s*-\s*(.+?)\s*\)\s*', str(column), re.I)
        ),
        None,
    )

    if direct_site_column is not None:
        site_source = df[direct_site_column].astype('string')
        output['Site'] = site_source.mask(site_source.str.strip().eq(''), pd.NA)
        output['Site'] = output['Site'].fillna(PARTNER_LABEL)
    elif reported_site_header is not None:
        site_match = re.fullmatch(
            r'\s*Site\s*\(\s*(F?\d+)\s*-\s*(.+?)\s*\)\s*',
            str(reported_site_header),
            re.I,
        )
        header_site = '{} - {}'.format(site_match.group(1), site_match.group(2))
        header_site_source = pd.Series(header_site, index=output.index, dtype='string')
        reported_sites = df[reported_site_header].astype('string')
        is_store_value = reported_sites.str.match(r'^\s*F?\d+\s*-\s*.+?\s*$', na=False)
        site_source = reported_sites.where(is_store_value, header_site_source)
        output['Site'] = site_source
    else:
        output['Site'] = PARTNER_LABEL
        site_source = pd.Series(pd.NA, index=output.index, dtype='string')

    site_details = output['Site'].astype('string').str.extract(r'^\s*F?(\d+)\s*-\s*(.+?)\s*$')
    output['StoreID'] = pd.to_numeric(site_details[0], errors='coerce').astype('Int64')
    output['StoreName'] = site_details[1]
    output['ID pdt fournisseur'] = output['ID pdt fournisseur'].replace(
        'VMA1000-1000S', 'VMA1000-10000S'
    )
    return output[['ID pdt fournisseur', 'Dénomination produit', 'Sales', 'Stock',
                   'EAN', 'Site', 'StoreID', 'StoreName', 'Date']]


def BOULANGER_FR_SAP_T_2(filepath=None):
    try:
        source_dir = filepath if filepath else input_folder_path
        # Ensure output folder exists (tests rely on this being created)
        os.makedirs(output_folder_path, exist_ok=True)

        if os.path.isfile(source_dir):
            input_dir = os.path.dirname(source_dir)
            files_to_process = [os.path.basename(source_dir)]
        else:
            input_dir = source_dir
            try:
                files_to_process = [
                    file for file in os.listdir(input_dir)
                    if file.lower().endswith(('.xlsx', '.xls')) and not file.startswith('~')
                ]
            except FileNotFoundError:
                files_to_process = []
            if not files_to_process:
                get_run_logger().info("No Excel file found in input folder")
                return "Success, BOULANGER_FR_SAP_T-2 Processed!"

        separator_POS = '\\t'
        separator_INV = '\\t'
        get_run_logger().info(os.listdir(input_dir))
        get_run_logger().info('FILE ENTERING THE POS OR INV FILE CHECKING STAGE')
        for file in files_to_process:
            file_name = file.split('.')[0]
            filename_output = file_name + '.xlsx'
            get_run_logger().info(file)
            if 'ventes semaine' in file.lower():
                get_run_logger().info("POS FILE")
                if file.endswith('.xls') or file.endswith('.XLS'):
                    get_run_logger().info('Trying to read file using utf-16 LE encoding')
                    input_file_path = _join_path(input_dir, file)
                    if os.path.exists(input_file_path):
                        get_run_logger().info("Exists")
                    else:
                        get_run_logger().info("Doesn't exists")
                        get_run_logger().info(input_file_path, 'is not found using os.path.exists')
                    try:
                        df = pd.read_csv(input_file_path, sep=separator_POS, encoding='utf-16 LE', engine='python')
                        get_run_logger().info('Read', file, 'using the utf-16 LE encoding')
                    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as e:
                        get_run_logger().info(f"csv read failed ({e}), falling back to read_excel")
                        df = pd.read_excel(os.path.join(input_dir, file))
                    df = _apply_signed_values_to_frame(df, r'Quantité vendue', 'Quantité vendue')

                    df = _build_pos_layout(df)
                    get_run_logger().info(df)
                    filepath_output = os.path.join(output_folder_path, filename_output)
                    get_run_logger().info('csv to excel conversion')
                    writer = pd.ExcelWriter(filepath_output, engine='xlsxwriter')
                    df.to_excel(writer, sheet_name=sheetname_POS, index=False)  #
                    writer.close()
                    try:
                        os.remove(os.path.join(input_dir, file))
                    except OSError as e:
                        get_run_logger().info(f"Warning: failed to remove {file}: {e}")
                elif file.endswith('.XLSX') or file.endswith('.xlsx'):
                    df1 = pd.read_excel(os.path.join(input_dir, file))
                    df1 = _apply_signed_values_to_frame(df1, r'Quantité vendue', 'Quantité vendue')
                    df1 = _build_pos_layout(df1)
                    get_run_logger().info('preparing file for conversion to excel')
                    filepath_output = os.path.join(output_folder_path, filename_output)
                    writer = pd.ExcelWriter(filepath_output, engine='xlsxwriter')
                    df1.to_excel(writer, sheet_name=sheetname_POS, index=False)  #
                    writer.close()
                    try:
                        os.remove(os.path.join(input_dir, file))
                    except OSError as e:
                        get_run_logger().info(f"Warning: failed to remove {file}: {e}")
                else:
                    get_run_logger().info('The given file:', file, 'is not a .xlsx or .xls format. Please check the format')

            elif 'stocks semaine' in file.lower():
                get_run_logger().info('INV FILE')
                if file.endswith('.xls') or file.endswith('.XLS'):
                    get_run_logger().info('Trying to read file with UTF-16 LE encoding')
                    input_file_path = _join_path(input_dir, file)
                    if os.path.exists(input_file_path):
                        get_run_logger().info("Exists")
                    else:
                        get_run_logger().info("Doesn't exists")
                        get_run_logger().info(input_file_path, 'is not found using os.path.exists')
                    try:
                        df = pd.read_csv(input_file_path, sep=separator_INV, encoding='utf-16 LE', engine='python')
                        get_run_logger().info('Read file with UTF-16 LE encoding')
                    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as e:
                        get_run_logger().info(f"csv read failed ({e}), falling back to read_excel")
                        df = pd.read_excel(os.path.join(input_dir, file))
                    df = _apply_signed_values_to_frame(df, r'Stock Total', 'Stock Total')
                    get_run_logger().info(df)
                    filepath_output = os.path.join(output_folder_path, filename_output)
                    writer = pd.ExcelWriter(filepath_output, engine='xlsxwriter')
                    df.to_excel(writer, sheet_name=sheetname_INV, index=False)  # ,
                    writer.close()
                    try:
                        os.remove(os.path.join(input_dir, file))
                    except OSError as e:
                        get_run_logger().info(f"Warning: failed to remove {file}: {e}")
                elif file.endswith('.XLSX') or file.endswith('.xlsx'):
                    df1 = pd.read_excel(os.path.join(input_dir, file))
                    df1 = _apply_signed_values_to_frame(df1, r'Stock Total', 'Stock Total')
                    filepath_output = os.path.join(output_folder_path, filename_output)
                    writer = pd.ExcelWriter(filepath_output, engine='xlsxwriter')
                    df1.to_excel(writer, sheet_name=sheetname_INV, index=False)  # ,sheet_name=sheetname_INV
                    writer.close()
                    try:
                        os.remove(os.path.join(input_dir, file))
                    except OSError as e:
                        get_run_logger().info(f"Warning: failed to remove {file}: {e}")
                else:
                    get_run_logger().info('The given file:', file, 'is not a .xlsx or .xls format. Please check the format')
            else:
                get_run_logger().info('The file:', file, 'not able to process!!Check if the filename is changed')
        S = "Success, BOULANGER_FR_SAP_T-2 Processed!"

    except Exception as e:
        S = "Exception occured in BOULANGER_FR_SAP_T-2 " + str(e)
    return S


if __name__ == "__main__":
    BOULANGER_FR_SAP_T_2()