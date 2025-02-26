from calendar import c
import re
import argparse
import copy
from unicodedata import decimal
import psycopg2
import pandas as pd
import json
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

citas = dtes = None

def read_citas_and_dtes():
    try:
        global citas, dtes
        range_name = 'Citas!A:L'
        result = sheets_api.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        citas = result.get('values', [])
        range_name = 'DTEs!A:L'
        result = sheets_api.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        dtes = result.get('values', [])
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

    
def create_and_copy_rows_to_tabs(fee, first_provider):
    data = citas
    data[0].append("id-vlookup1")
    data[0].append("id-boleta")
    data[0].append("largo-rut")
    data[0].append("folio")
    data[0].append("monto-boleta")
    data[0].append("monto-servicios")
    data[0].append("valor")
    data[0].append("voucher pos")
    data[0].append("propina pos")
    data[0].append("participantes venta")
    unique_values = sorted(list(set(row[4] for row in data[1:])))
    
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
        sheet_id = create_tab(value)

        # Copy rows with the corresponding value in column B to the new tab
        filtered_rows = [data[0]] + [row for row in data[1:] if row[4] == value]
        if not filtered_rows:
            continue

        function01 = "=IF(A@@<>\"\"; CONCAT(A@@;CONCAT(\"-\";VLOOKUP(E@@;Emisores!$A$1:$B$100;2;FALSE)));\"\")"
        function02 = "=IF(M@@<>\"\"; IFERROR(HYPERLINK(VLOOKUP(M@@;DTEs!A:L;12;FALSE);VLOOKUP(M@@;DTEs!A:L;11;FALSE)); IFERROR(VLOOKUP(CONCAT(CONCAT(A@@;\"-\");E@@);Errores!A:F;6;FALSE);\"Sin DTE\"));\"\")"
        function03 = "=IF(M@@<>\"\"; LEN(VLOOKUP(M@@;DTEs!A:L;6;FALSE))-2;\"\")"
        function04 = "=IF(M@@<>\"\"; INT(LEFT(RIGHT(N@@;LEN(N@@));5));\"\")"
        function05 = "=IF(M@@<>\"\"; VLOOKUP(M@@;DTEs!A:L;10;FALSE);\"\")"
        function06 = "=IF(M@@<>\"\"; CEILING(SUMIFS(G:G;A:A;A@@));\"\")"
        function07 = "=IFERROR(Q@@/R@@;\"\")"
        function08 = "=IFERROR(VLOOKUP(A@@;Transacciones!A:F;3;false);\"\")"
        function09 = "=IFERROR(VLOOKUP(A@@;Transacciones!A:F;5;false);\"\")"
        function10 = "=COUNTUNIQUEIFS(Citas!E:E;Citas!A:A;A@@)"
        ri = 2
        function1_column = 'M'

        for row in filtered_rows[1:]:
            # fill empty cells
            while len(row) < (ord(function1_column) - ord('A')):
                row.append('')
            # append functions
            row.append(function01.replace("@@", str(ri)))
            row.append(function02.replace("@@", str(ri)))
            row.append(function03.replace("@@", str(ri)))
            row.append(function04.replace("@@", str(ri)))
            row.append(function05.replace("@@", str(ri)))
            row.append(function06.replace("@@", str(ri)))
            row.append(function07.replace("@@", str(ri)))
            row.append(function08.replace("@@", str(ri)))
            row.append(function09.replace("@@", str(ri)))
            row.append(function10.replace("@@", str(ri)))
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
    
def find_column_height(tab_name, column):

        # Define the range in which you want to search for the last non-empty cell
        range_ = f'{tab_name}!{column}:{column}'

        # Get all values in column 'A'
        response = sheets_api.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        values = response.get('values', [])
        
        # Find the last non-empty cell in column 'A'
        return len(values)

def create_tab(tab_name, freeze_headers=True):
    if tab_name in tabs:
        try:
            range_all = f'{tab_name}!A1:Z'
            sheets_api.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_all, body={}).execute()
        except HttpError as error:
            print(f"Could not clean tab {tab_name}: {error}")
        return tabs[tab_name]
    
    create_tab_request = {
        "addSheet": {
            "properties": {
                "title": tab_name
            }
        }
    }

    try:
        x = sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [create_tab_request]}).execute()
        sheet_id = x["replies"][0]["addSheet"]["properties"]["sheetId"]
        tabs[tab_name] = sheet_id
        if freeze_headers:
            freeze_request = {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'gridProperties': {
                            'frozenRowCount': 1
                        }
                    },
                    'fields': 'gridProperties.frozenRowCount'
                }
            }
            sheets_api.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [freeze_request]}).execute()
        return sheet_id
    except HttpError as error:
        print(f"Could not create tab {tab_name}: {error}")
    return None
        

def create_company_tabs(ruts):

    for rut_and_location in ruts:
        if not rut_and_location:
            continue
        rut, location = rut_and_location.split("/")
        tab_name = f"{location}-{rut}"
        print(f"trabajando en '{rut}'")

        create_tab(tab_name)
        
        # Copy payment_ids from Citas
        sheets_api.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{tab_name}!A1:A2',
            valueInputOption='USER_ENTERED',
            body={'values': [
                ['id-payment'], 
                [f'=UNIQUE(FILTER(Citas!A2:A; Citas!C2:C="{location}"))']
            ]}
        ).execute()

        last_non_empty_cell = find_column_height(tab_name, 'A')

        arr = [['total', 'id-vlookup', 'monto-boleta', 'dte']]
        
        arr.extend([
            [
                f'=SUMIF(Citas!A:A;A{str(i+2)};Citas!F:F)',
                f'=CONCAT($A{str(i+2)};"-{rut}")',
                f'=SUMIF(DTEs!$A:$A;C{str(i+2)};DTEs!$J:$J)',
                f'=IFERROR(VLOOKUP(C{str(i+2)};DTEs!A:L;12;FALSE);VLOOKUP(CONCAT(CONCAT(A{str(i+2)};"-");\"{location}\");Errores!A:F;6))',
            ] for i in range(last_non_empty_cell-1)
        ])

        sheets_api.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{tab_name}!B1:F' + str(last_non_empty_cell+1),
            valueInputOption='USER_ENTERED',
            body={'values': arr}
        ).execute()


def create_catalogo_tabs():

    tab_name = "Catalogo"
    print(f"trabajando en '{tab_name}'")

    create_tab(tab_name)
    dc = copy.deepcopy(citas[1:])
    for row in dc:
        row.extend(['', '', ''])
    dc.extend([
        [d[1], '', '', '', '', '', '0', '', '', '', '', '', '', d[6], d[5], d[9]]
     for d in dtes[1:]])
    dc = sorted(dc, key=lambda row: row[0] + '-' + row[4], reverse=True)
    arr = [['payment_id', 'local', 'cliente', 'fecha', 'proveedor', 'servicio', 'subtotal_ítem', '', 'emisor', 'rut_emisor', 'subtotal_dte']]

    current_pid = None
    current_item_total = 0
    for r in range(len(dc)):
        if(dc[r][0] != current_pid):
            current_pid = dc[r][0]
            arr.extend([[dc[r][0], dc[r][2], dc[r][9]]])
            current_item_total = 0
        if(dc[r][6] != '0'):
            arr.extend([['', '', '', dc[r][1], dc[r][4], dc[r][11], dc[r][6]]])
            current_item_total += float(dc[r][6])
        else:
            arr.extend([['', '', '', '', '', '', '', '', dc[r][13], dc[r][14], dc[r][15]]])
        if(r+1 == len(dc) or dc[r+1][0]!= current_pid):
            arr.extend([['', '', '', '', '', 'Total ítems', current_item_total]])

    sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{tab_name}!A1:K' + str(len(arr)),
        valueInputOption='USER_ENTERED',
        body={'values': arr}
    ).execute()
    
def create_cruce_basico():

    tab_name = f"Cruce"
    print(f"trabajando en '{tab_name}'")
    create_tab(tab_name)

    # Insert payment ids
    sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{tab_name}!A1:A2',
        valueInputOption='USER_ENTERED',
        body={'values': [[
            'payment_id'
        ], ['=UNIQUE(Citas!A2:A)']]}
    ).execute()

    payment_id_count = find_column_height(tab_name, 'A') - 1

    sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{tab_name}!B1:J1',
        valueInputOption='USER_ENTERED',
        body={'values': [[
            'location', 
            'provider-count',
            'BHE count',
            'BA count',
            'Falta BHE',
            'Falta BA',
            'Falta DTE',
            'Emisor',
            'Error'
        ]]}
    ).execute()


    values = [
        [
            f'=VLOOKUP(A{str(i+2)};Citas!A:L;3;FALSE)',
            f'=COUNTUNIQUEIFS(Citas!D:D;Citas!A:A;A{str(i+2)})',
            f'=COUNTIFs(DTEs!B:B;A{str(i+2)};DTEs!E:E;"boleta_honorarios")',
            f'=COUNTIFs(DTEs!B:B;A{str(i+2)};DTEs!E:E;"boleta")',
            f'=C{str(i+2)}>D{str(i+2)}',
            f'=AND(C{str(i+2)}>0;E{str(i+2)}=0)',
            f'=OR(F{str(i+2)};G{str(i+2)})',
            f'=IF(H{str(i+2)};IFERROR(VLOOKUP(A{str(i+2)};Errores!B:F;4;false);"");"")',
            f'=IF(H{str(i+2)};IFERROR(VLOOKUP(A{str(i+2)};Errores!B:F;5;false);"No hubo error");"")'
        ] for i in range(payment_id_count)
    ]

    sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{tab_name}!B2:J' + str(payment_id_count+1),
        valueInputOption='USER_ENTERED',
        body={'values': values}
    ).execute()

def query_issuers(company_id, date_from, date_to):
    return f"""
    -- Búsqueda Emisores
    with real_ruts as (select p.company_id, d.issuer_identification as rut, d.issuer_name, count(1) as q
                   from dtes d
                            left join payments p on d.payment_id = p.id
                   where p.company_id = {company_id}
                     and p.paid_at >= '{date_from}'
                     and p.paid_at < '{date_to}'
                     and status = 'completed'
                     and version = 'final'
                   group by 1, 2, 3
                   order by 4 desc),
     raw_theoretical_ruts as (select company_id,
                                     json_object_keys(params -> 'dtemite') AS rut,
                                     params ->> 'name'                     as issuer_name,
                                     0                                     as q
                              from company_values
                              where company_id = {company_id}
                              union all
                              select company_id, params ->> 'rut' as rut, params ->> 'name' as issuer_name, 0 as q
                              from provider_values
                              where company_id = {company_id}),
     theoretical_ruts as (select *
                          from raw_theoretical_ruts
                          where rut is not null)
select  
        coalesce(r.issuer_name, t.issuer_name) as issuer_name, 
        rut
from real_ruts r
         full outer join theoretical_ruts t using (company_id, rut);"""

def query_dtes(company_id, date_from, date_to):
    return f"""
    -- Búsqueda DTEs
    select p.payment_id::text || '-' || d.issuer_identification as vlookup_id,
        p.payment_id,
        d.id                                                 as tready_id,
        to_char(d.issued_at, 'yyyy-MM-dd HH:mi')             as fecha_emision,
        d.tax_receipt_type                                   as tipo_dte,
        d.issuer_identification                              as emisor_rut,
        d.issuer_name                                        as emisor_nombre,
        d.customer_identification                            as receptor_rut,
        d.customer_name                                      as receptor_nombre,
        d.total::int                                         as monto,
        '''' || ((d.document::json) ->> 'number')::text      as folio,
        (d.document::json) ->> 'url'                         as pdf
    from dtes d
            join payments p on (p.id = d.payment_id)
    where p.company_id = {company_id}
    and p.paid_at >= '{date_from}'
    and p.paid_at < '{date_to}'
    and status = 'completed'
    and version = 'final'
    order by p.payment_id desc; """

def query_citas(company_id, date_from, date_to):
    return f"""
    -- Búsqueda Citas
    select payment_id,
        to_char(booking_start_time, 'yyyy-MM-dd HH:mi')             as booking_start_time,
        location,
        provider_id,
        provider_name,
        booking_id,
        booking_price,
        booking_status,
        client_id,
        client_name,
        service_id,
        service_name
    from dwh.augmented_bookings
    where company_id = {company_id}
    and booking_start_time >= '{date_from}'
    and booking_start_time < '{date_to}'
    and payment_id is not null
    order by booking_start_time desc;"""

def query_transacciones(company_id, date_from, date_to):
    return f"""
    select 
        s.payment_id, 
        t.id as transaction_id, 
        t.external_reference, 
        t.amount::int, 
        t.tip::int,
        to_char(p.payment_date, 'yyyy-MM-dd HH24:mi') as payment_date
    from transactions t
    left join payment_requests pr on t.payment_request_id = pr.id
    left join sales s on pr.cart_id = s.cart_id
    left join payments p on s.payment_id = p.id
    where t.company_id = {company_id}
    and t.paid_at >= '{date_from}'
    and t.paid_at < '{date_to}'
    and t.paymentable_id = 40
    order by t.created_at desc;"""

def query_errores(company_id, date_from, date_to):
    return f"""
    -- errores
    with all_errors as (
    select 
        p.payment_id || '-' || d.issuer_name as vlookup_id,
        p.payment_id,
        d.issuer_identification,
        d.issuer_name,
        case when d.status || '-' || d.version = 'error-final' then d.error ->> 'description' else null end as error,
        to_char(d.updated_at, 'yyyyMMdd HH:mi') as updated_at,
        row_number() over (partition by p.payment_id, d.issuer_name order by d.updated_at desc) as rn
    from dtes d
            left join payments p on d.payment_id = p.id
    where p.company_id = {company_id}
    and p.paid_at >= '{date_from}'
    and p.paid_at < '{date_to}'
    and d.error is not null
    order by 1, 4)
    select 
        vlookup_id,
        payment_id,
        updated_at,
        issuer_identification,
        issuer_name,
        error
    from all_errors
    where rn = 1
    order by 1, 3;"""

def connect_and_fetch_data(query, hostname, username, password, database):
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = conn.cursor() 
    cur.execute(query)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    return [rows, headers]

tabs = {}
def load_existing_tabs():
    try:
        response = sheets_api.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as err:
        print(f"An HTTP error occurred: {err}")

    for sheet in response['sheets']:
        k = sheet['properties']['title']
        v = sheet['properties']['sheetId']
        tabs[k] = v


# Load the credentials from a JSON file
with open('db_credentials.json') as f:
    db_credentials = json.load(f)
    

def load_data(tab, db_key, query):
    print(f"Cargando '{tab}'")
    create_tab(tab)
    rows, headers = connect_and_fetch_data(query, db_credentials[db_key]["host"], db_credentials[db_key]["user"], db_credentials[db_key]["pass"], db_credentials[db_key]["db"])
    values = [headers] + rows
    body = {
        'values': values
    }
    result = sheets_api.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=tab,
        valueInputOption='USER_ENTERED', body=body).execute()

def main(company_id, date_from, date_to, url, cruce, fee, report_bhe, first_provider, ruts):
    global spreadsheet_id, citas
    spreadsheet_id = get_spreadsheet_id_from_url(url)
    load_existing_tabs()

    if company_id and date_from and date_to:
        load_data("DTEs", "tready", query_dtes(company_id, date_from, date_to))
        load_data("Citas", "dwh", query_citas(company_id, date_from, date_to))
        load_data("Errores", "tready", query_errores(company_id, date_from, date_to))
        load_data("Transacciones", "ap", query_transacciones(company_id, date_from, date_to))
        load_data("Emisores", "tready", query_issuers(company_id, date_from, date_to))
        
    read_citas_and_dtes()
    create_catalogo_tabs()

    if (cruce or ruts or report_bhe) and not fee:
        raise Exception("Se requiere indicar el fee del prestador con la opción -f o --fee")

    if report_bhe or ruts or cruce:
        create_cruce_basico()

    if ruts:
        create_company_tabs(ruts)

    if report_bhe:
        if citas:
            create_and_copy_rows_to_tabs(fee, first_provider)
        else:
            print("No data found in the source tab.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generador de reportes de Tready')
    parser.add_argument('-u',  '--url',          type=str, required=True, help='URL del reporte sin incluir /edit#gid=...')
    parser.add_argument('-c',  '--cruce',        action='store_true',     help='Calcular cruce general')
    parser.add_argument('-f',  '--fee',          type=float,              help='Comisión del prestador')
    parser.add_argument('-re', '--ruts-empresa', nargs='+',               help='RUTs de empresa y nombres de local en formato RUT/Location')
    parser.add_argument('-bh', '--report-bhe',   action='store_true',     help='Crear hojas por prestador')
    parser.add_argument('-s',  '--skip-until',   type=str,                help='Comenzar con este prestador')
    parser.add_argument('-ci', '--company-id',   type=str,                help='Extraer datos de Company ID')
    parser.add_argument('-df', '--date-from',    type=str,                help='Extraer desde en formato yyyyMMdd')
    parser.add_argument('-dt', '--date-to',      type=str,                help='Extraer hasta (no inclusivo) en formato yyyyMMdd')

    args = parser.parse_args()
    main(args.company_id, args.date_from, args.date_to, args.url, args.cruce, args.fee, args.report_bhe, args.skip_until, args.ruts_empresa)
