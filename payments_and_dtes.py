import re
import argparse
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

    
def create_and_copy_rows_to_tabs(data, fee):
    data[0].append("id-vlookup1")
    data[0].append("id-boleta")
    data[0].append("largo-rut")
    data[0].append("folio")
    data[0].append("monto-boleta")
    data[0].append("monto-servicios")
    data[0].append("valor")
    unique_values = sorted(list(set(row[3] for row in data[1:])))
    print(unique_values)
    tabs = 1
    for value in unique_values:
        #if tabs > 1:
            #break
        if not value:
            continue
        print(f"trabajando en '{value}'")
        tabs = tabs + 1
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

        function1 = "=IF(L@@<>\"\"; CONCAT(L@@;CONCAT(\"-\";VLOOKUP(D@@;Emisores!$A$1:$B$30;2;FALSE)));\"\")"
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
        

def main(url, fee, iss, ruts):
    global spreadsheet_id
    spreadsheet_id = get_spreadsheet_id_from_url(url)
    if iss:
        unique_issuers_and_ruts('DTEs')
    data = get_data_from_source_tab()
    if data:
        create_and_copy_rows_to_tabs(data, fee)
    else:
        print("No data found in the source tab.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process URL and fee.')
    parser.add_argument('--url', type=str, required=True, help='The URL to process.')
    parser.add_argument('--fee', type=float, required=True, help='The fee to process.')
    parser.add_argument('--regenerate-list', action='store_true', help='Regenerate issuer list, yes/no')
    parser.add_argument('--ruts-empresa', nargs='+', help='RUTs de empresa')

    args = parser.parse_args()
    main(args.url, args.fee, args.regenerate_list, args.ruts_empresa)
