import re
import argparse
import copy
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError

# Set up Google Sheets API credentials
SERVICE_ACCOUNT_FILE = 'rutificador-384117-fb50f95b19f7.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Load your service account key and create an API client
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheets_api = discovery.build('sheets', 'v4', credentials=credentials)

def get_spreadsheet_id_from_url(url):
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None

spreadsheet_id = "AAAAAAAAAA"

SOURCE_TAB_NAME = 'Citas'

def get_data_from_source_tab():
    try:
        range_name = f'{SOURCE_TAB_NAME}!A:L'
        result = sheets_api.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        return result.get('values', [])
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

#not used
def tab_exists(tab_name):
    try:
        sheet_metadata = sheets_api.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        for sheet in sheets:
            if sheet['properties']['title'] == tab_name:
                return True
        return False
    except HttpError as error:
        print(f"An error occurred: {error}")
        return False

def format_percentage_column(sheet_name, sheet_id, column_letter, decimal_places, row_count):
    start_range = f"{sheet_name}!{column_letter}1"
    end_range = f"{sheet_name}!{column_letter}{row_count}"  # Set a large enough row number to cover all rows in the sheet
    sheet_range = f"{start_range}:{end_range}"

    format_request = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": row_count,  # Set a large enough row number to cover all rows in the sheet
                "startColumnIndex": ord(column_letter) - ord("A"),
                "endColumnIndex": ord(column_letter) - ord("A") + 1,
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "PERCENT",
                        "pattern": f'0.{"".join(["0" for _ in range(decimal_places)])}%'
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    }

    try:
        sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [format_request]}).execute()
    except HttpError as error:
        print(f"An error occurred: {error}")

def apply_conditional_formatting(sheet_name, sheet_id, column_letter, row_count, fee):
    fee_from = "{:.2f}".format(fee - 0.01).replace('.', ',')
    fee_to = "{:.2f}".format(fee + 0.01).replace('.', ',')
    start_range = f"{sheet_name}!{column_letter}1"
    end_range = f"{sheet_name}!{column_letter}{row_count}"  # Set a large enough row number to cover all rows in the sheet
    sheet_range = f"{start_range}:{end_range}"

    conditional_formatting_request = {
        "requests": [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "endRowIndex": row_count,  # Set a large enough row number to cover all rows in the sheet
                                "startColumnIndex": ord(column_letter) - ord("A"),
                                "endColumnIndex": ord(column_letter) - ord("A") + 1,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [
                                    {
                                        "userEnteredValue": f"=AND({column_letter}2>={fee_from}; {column_letter}2<={fee_to})"
                                    }
                                ],
                            },
                            "format": {
                                "backgroundColor": {
                                    "red": 0.0,
                                    "green": 1.0,
                                    "blue": 0.0,
                                },
                            },
                        },
                    },
                    "index": 0,
                },
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "endRowIndex": row_count,  # Set a large enough row number to cover all rows in the sheet
                                "startColumnIndex": ord(column_letter) - ord("A"),
                                "endColumnIndex": ord(column_letter) - ord("A") + 1,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [
                                    {
                                        "userEnteredValue": f"=OR({column_letter}2<{fee_from}; {column_letter}2>{fee_to})"
                                    }
                                ],
                            },
                            "format": {
                                "backgroundColor": {
                                    "red": 1.0,
                                    "green": 1.0,
                                    "blue": 0.0,
                                },
                            },
                        },
                    },
                    "index": 1,
                },
            },
        ]
    }

    try:
        sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=conditional_formatting_request).execute()
    except HttpError as error:
        print(f"An error occurred: {error}")




def get_sheet_id(sheet_name):
    sheet_metadata = sheets_api.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_id = None
    for sheet in sheets:
        if sheet['properties']['title'] == sheet_name:
            sheet_id = sheet['properties']['sheetId']
            break
    return sheet_id

def unique_issuers_and_ruts(sheet_name):
    range_name = f'{sheet_name}!F:G'
    result = sheets_api.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    rows = result.get('values', [])

    # Process the data
    unique_people = {}
    for row in rows:
        try:
            rut, name = row
        except ValueError as error:
            print(f"{error} en fila {row}")
            continue
        if name not in unique_people:
            unique_people[name] = rut
    sorted_names = sorted(unique_people.keys())

    # Create a new tab
    new_sheet_title = 'Emisores'
    new_sheet = {
        'requests': [
            {
                'addSheet': {
                    'properties': {
                        'title': new_sheet_title
                    }
                }
            }
        ]
    }
    sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=new_sheet).execute()

    # Write the data to the new tab
    new_range = f'{new_sheet_title}!A:B'
    body = {
        'range': new_range,
        'majorDimension': 'ROWS',
        'values': [[name, unique_people[name]] for name in sorted_names]
    }
    sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=new_range,
        valueInputOption='RAW', body=body).execute()

    
def create_and_copy_rows_to_tabs(data, fee, first_provider):
    data[0].append("id-vlookup1")
    data[0].append("id-boleta")
    data[0].append("largo-rut")
    data[0].append("folio")
    data[0].append("monto-boleta")
    data[0].append("monto-servicios")
    data[0].append("valor")
    unique_values = sorted(list(set(row[3] for row in data[1:])))
    
    process_all = (first_provider is None)
    for value in unique_values:
        if not value:
            continue
        if not process_all:
            process_all = (value == first_provider)
            if not process_all:
                print(f"skipping {value}")
                continue
        print(f"trabajando en '{value}'")
        # Create a new tab
        create_tab_request = {
            "addSheet": {
                "properties": {
                    "title": value
                }
            }
        }
        try:
            sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [create_tab_request]}).execute()
        except HttpError as error:
            print(f"Could not create tab {value}")
            continue
        
        sheet_id = get_sheet_id(value)

        # Copy rows with the corresponding value in column B to the new tab
        filtered_rows = [data[0]] + [row for row in data[1:] if row[3] == value]
        if not filtered_rows:
            continue

        print(f"{value} tiene {len(filtered_rows)} filas")

        function1 = "=IF(L@@<>\"\"; CONCAT(L@@;CONCAT(\"-\";VLOOKUP(D@@;Emisores!$A$1:$B$100;2;FALSE)));\"\")"
        function2 = "=IF(M@@<>\"\"; HYPERLINK(VLOOKUP(M@@;DTEs!A:L;12;FALSE);VLOOKUP(M@@;DTEs!A:L;11;FALSE));\"\")"
        function3 = "=IF(M@@<>\"\"; LEN(VLOOKUP(M@@;DTEs!A:L;6;FALSE))-2;\"\")"
        function4 = "=IF(M@@<>\"\"; INT(LEFT(RIGHT(N@@;LEN(N@@)-O@@);5));\"\")"
        function5 = "=IF(M@@<>\"\"; VLOOKUP(M@@;DTEs!A:L;10;FALSE);\"\")"
        function6 = "=IF(M@@<>\"\"; CEILING(SUMIFS(F:F;L:L;L@@));\"\")"
        function7 = "=IFERROR(Q@@/R@@;\"\")"
        ri = 2
        function1_column = 'M'

        for row in filtered_rows[1:]:
            # fill empty cells
            while len(row) < (ord(function1_column) - ord('A')):
                row.append('')
            # append functions
            row.append(function1.replace("@@", str(ri)))
            row.append(function2.replace("@@", str(ri)))
            row.append(function3.replace("@@", str(ri)))
            row.append(function4.replace("@@", str(ri)))
            row.append(function5.replace("@@", str(ri)))
            row.append(function6.replace("@@", str(ri)))
            row.append(function7.replace("@@", str(ri)))
            ri = ri + 1

        # Update the range_name to cover the entire range being written
        last_column = chr(ord('A') + max(len(row) for row in filtered_rows) - 1)
        range_name = f'{value}!A1:{last_column}{len(filtered_rows)}'
        body = {
            'values': filtered_rows
        }
        try:
            sheets_api.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="USER_ENTERED", body=body).execute()
            format_percentage_column(value, sheet_id, 'S', 1, len(filtered_rows))
            apply_conditional_formatting(value, sheet_id, 'S', len(filtered_rows), fee)  # Apply conditional formatting to column 'S'
        except HttpError as error:
            print(f"An error occurred: {error}")
    
def create_company_tabs(ruts):

    for rut_and_location in ruts:
        if not rut_and_location:
            continue
        rut, location = rut_and_location.split("/")
        tab_name = f"Empresa-{rut}"
        print(f"trabajando en '{rut}'")
        # Create a new tab
        create_tab_request = {
            "addSheet": {
                "properties": {
                    "title": tab_name
                }
            }
        }
        try:
            sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [create_tab_request]}).execute()
        except HttpError as error:
            print(f"Could not create tab {tab_name}")
            continue

        sheet_id = get_sheet_id(tab_name)

        update_cells_request = {
            "requests": [
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1
                        },
                        "rows": [
                            {
                                "values": [
                                    {
                                        "userEnteredValue": {
                                            "stringValue": "id-payment"
                                        }
                                    }
                                ]
                            }
                        ],
                        "fields": "userEnteredValue"
                    }
                },
                {
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1
                        },
                        "rows": [
                            {
                                "values": [
                                    {
                                        "userEnteredValue": {
                                            "formulaValue": f"=UNIQUE(FILTER(Citas!L2:L; Citas!B2:B=\"{location}\"))"
                                        }
                                    }
                                ]
                            }
                        ],
                        "fields": "userEnteredValue"
                    }
                }
            ]
        }

        try:
            sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=update_cells_request).execute()
        except HttpError as error:
            print(f"An error occurred: {error}")

        
        # Define the range in which you want to search for the last non-empty cell
        range_ = f'{tab_name}!A:A'

        # Get all values in column 'A'
        response = sheets_api.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        values = response.get('values', [])
        
        # Find the last non-empty cell in column 'A'
        last_non_empty_cell = len(values)

        # Insert "ASD" as the header of the second column
        sheets_api.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{tab_name}!B1:F1',
            valueInputOption='USER_ENTERED',
            body={'values': [[
                'total', 
                'id-vlookup',
                'monto-boleta',
                'dte'
            ]]}
        ).execute()


        # Prepare the formula for each cell in column 'B'
        values = [
            [
                f'=SUMIF(Citas!L:L;A{str(i+2)};Citas!F:F)',
                f'=CONCAT($A{str(i+2)};"-{rut}")',
                f'=SUMIF(DTEs!$A:$A;C{str(i+2)};DTEs!$J:$J)',
                f'=VLOOKUP(C{str(i+2)};DTEs!A:L;12;FALSE)',
            ] for i in range(last_non_empty_cell-1)
        ]

        # Insert the formulas starting from B2 until the last cell in column 'B' that has a corresponding value in column 'A'
        sheets_api.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{tab_name}!B2:F' + str(last_non_empty_cell+1),
            valueInputOption='USER_ENTERED',
            body={'values': values}
        ).execute()

def main(url, fee, report_bhe, first_provider, iss, ruts):
    global spreadsheet_id
    spreadsheet_id = get_spreadsheet_id_from_url(url)
    if iss:
        unique_issuers_and_ruts('DTEs')

    data = get_data_from_source_tab()

    if report_bhe:
        if data:
            create_and_copy_rows_to_tabs(copy.deepcopy(data), fee, first_provider)
        else:
            print("No data found in the source tab.")

    if ruts:
        create_company_tabs(ruts)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process URL and fee.')
    parser.add_argument('--url', type=str, required=True, help='The URL to process.')
    parser.add_argument('--fee', type=float, required=True, help='The fee to process.')
    parser.add_argument('--discover-issuers', action='store_true', help='Regenerate issuer list')
    parser.add_argument('--ruts-empresa', nargs='+', help='RUTs de empresa y Locations en formato RUT/Location')
    parser.add_argument('--report-bhe', action='store_true', help='Create report for providers')
    parser.add_argument('--first-provider', type=str, help='Start with this provider')

    args = parser.parse_args()
    main(args.url, args.fee, args.report_bhe, args.first_provider, args.regenerate_list, args.ruts_empresa)
