#
#        Repsly2Excel
#
#        Author: Benjamin Cance
#
#        Email: bjc@tdx.li
#
#        Website: https://tdx.li
#
#        License: MIT License
#
#        Copyright © 2024 - Benjamin Cance
#        
#        Permission is hereby granted, free of charge, to any person obtaining a copy
#        of this software and associated documentation files (the "Software"), to deal
#        in the Software without restriction, including without limitation the rights
#        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#        copies of the Software, and to permit persons to whom the Software is
#        furnished to do so, subject to the following conditions:
#
#        The above copyright notice and this permission notice shall be included in all
#        copies or substantial portions of the Software.
#
#        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#        SOFTWARE.
#

import argparse
import base64
import concurrent.futures
import functools
import json
import logging
import os
import requests
import sys
import time

from openpyxl import Workbook, load_workbook
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

##############################################################################################################
# These should be in environment variables if deploying this for prod, but we're just testing at the moment ##
                                                                                                             #
API_USERNAME = "<API KEY USER>"                                                                              #
API_PASSWORD = "<API KEY PASS>"                                                                              #
BASE_URL = "https://api.repsly.com/v3/export"                                                                #
                                                                                                             #
##############################################################################################################

auth_header = base64.b64encode(f"{API_USERNAME}:{API_PASSWORD}".encode()).decode()
headers = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler('repsly_export.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

def log_function_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.debug(f"Finished {func.__name__}. Execution time: {end_time - start_time:.2f} seconds")
        
        if func.__name__ == 'fetch_data':
            if result:
                try:
                    #data_str = json.dumps(result, indent=2)[:1000]
                    #logger.debug(f"API Response Data (truncated to 1000 chars):\n{data_str}")
                    
                    if isinstance(result, dict):
                        for key, value in result.items():
                            if isinstance(value, list):
                                logger.debug(f"'{key}' contains {len(value)} items")
                            elif isinstance(value, dict):
                                logger.debug(f"'{key}' is a dictionary with {len(value)} key-value pairs")
                    elif isinstance(result, list):
                        logger.debug(f"Response is a list with {len(result)} items")
                except Exception as e:
                    logger.error(f"Error parsing API response: {str(e)}")
            else:
                logger.warning("API response is empty or None")
        
        if isinstance(result, str) and result.endswith('.xlsx'):
            try:
                wb = load_workbook(result)
                for sheet_name in wb.sheetnames:
                    sheet = wb[sheet_name]
                    logger.debug(f"Sheet '{sheet_name}' in {result} has {sheet.max_row} rows and {sheet.max_column} columns")
                    if sheet.max_row > 1:
                        first_data_row = list(sheet.iter_rows(min_row=2, max_row=2, values_only=True))[0]
                        logger.debug(f"First data row in '{sheet_name}': {first_data_row}")
                    else:
                        logger.warning(f"Sheet '{sheet_name}' in {result} has no data rows")
            except Exception as e:
                logger.error(f"Error analyzing {result}: {str(e)}")
        return result
    return wrapper

def save_last_ids(last_ids, filename='last_ids.json'):
    with open(filename, 'w') as f:
        json.dump(last_ids, f, indent=2)

def load_last_ids(filename='last_ids.json'):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return {}

@log_function_call
def fetch_data(url, params=None):
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error fetching data from {url}: {response.status_code}")
        return None

def process_field(value):
    if isinstance(value, list):
        return ', '.join(str(v) for v in value)
    elif isinstance(value, dict):
        return ', '.join(f"{k}:{v}" for k, v in value.items())
    return value

@log_function_call
def process_data(endpoint, key_name, headers, last_value=0, use_timestamp=False):
    filename = f"Repsly_{key_name}_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = key_name
    ws.append(headers)
    row_count = 0
    
    while True:
        url = f"{BASE_URL}/{endpoint}/{last_value}"
        data = fetch_data(url)
        
        if data and key_name in data:
            for item in data[key_name]:
                row = [process_field(item.get(header)) for header in headers]
                ws.append(row)
                row_count += 1
            
            meta = data.get('MetaCollectionResult', {})
            if use_timestamp:
                new_last_value = meta.get('LastTimeStamp')
            else:
                new_last_value = meta.get('LastID')
            
            if new_last_value is not None and new_last_value != last_value:
                last_value = new_last_value
            else:
                break
        else:
            logging.warning(f"No '{key_name}' found in data from {url}")
            break

    wb.save(filename)
    logging.info(f"{key_name} data saved to {filename}. Total rows: {row_count}")
    return filename, last_value

@log_function_call
def process_clients(last_id=0):
    headers = [
        "ClientID", "TimeStamp", "Code", "Name", "Active", "Tag", "Territory",
        "RepresentativeCode", "RepresentativeName", "StreetAddress", "ZIP", "City",
        "State", "Country", "Email", "Phone", "Mobile", "Website", "ContactName",
        "ContactTitle", "Note", "Status", "CustomFields", "PriceLists", "AccountCode"
    ]
    return process_data("clients", "Clients", headers, last_id, use_timestamp=False)

@log_function_call
def process_client_notes(last_id=0):
    headers = [
        "ClientNoteID", "TimeStamp", "DateAndTime", "RepresentativeCode",
        "RepresentativeName", "ClientCode", "ClientName", "StreetAddress",
        "ZIP", "ZIPExt", "City", "State", "Country", "Email", "Phone",
        "Mobile", "Territory", "Longitude", "Latitude", "Note", "VisitID"
    ]
    return process_data("clientnotes", "ClientNotes", headers, last_id, use_timestamp=False)

@log_function_call
def process_visits(last_timestamp=0):
    headers = [
        "VisitID", "TimeStamp", "Date", "RepresentativeCode", "RepresentativeName",
        "ExplicitCheckIn", "DateAndTimeStart", "DateAndTimeEnd", "ClientCode",
        "ClientName", "StreetAddress", "ZIP", "ZIPExt", "City", "State", "Country",
        "Territory", "LatitudeStart", "LongitudeStart", "LatitudeEnd", "LongitudeEnd",
        "PrecisionStart", "PrecisionEnd", "VisitStatusBySchedule", "VisitEnded"
    ]
    return process_data("visits", "Visits", headers, last_timestamp, use_timestamp=True)

@log_function_call
def process_retail_audits(last_id=0):
    headers = [
        "RetailAuditID", "RetailAuditName", "Cancelled", "ClientCode", "ClientName",
        "DateAndTime", "RepresentativeCode", "RepresentativeName", "ProductGroupCode",
        "ProductGroupName", "ProductCode", "ProductName", "Present", "Price",
        "Promotion", "ShelfShare", "ShelfSharePercent", "SoldOut", "Stock",
        "CustomFields", "Note", "VisitID"
    ]
    return process_data("retailaudits", "RetailAudits", headers, last_id, use_timestamp=False)

@log_function_call
def process_purchase_orders(last_id=0):
    headers = [
        "PurchaseOrderID", "TransactionType", "DocumentTypeID", "DocumentTypeName",
        "DocumentStatus", "DocumentStatusID", "DocumentItemAttributeCaption",
        "DateAndTime", "DocumentNo", "ClientCode", "ClientName", "DocumentDate",
        "DueDate", "RepresentativeCode", "RepresentativeName", "LineNo",
        "ProductCode", "ProductName", "UnitAmount", "UnitPrice", "PackageTypeCode",
        "PackageTypeName", "PackageTypeConversion", "Quantity", "Amount",
        "DiscountAmount", "DiscountPercent", "TaxAmount", "TaxPercent", "TotalAmount",
        "ItemNote", "DocumentItemAttributeName", "DocumentItemAttributeID",
        "SignatureURL", "Note", "Taxable", "VisitID", "StreetAddress", "ZIP",
        "ZIPExt", "City", "State", "Country", "CountryCode", "CustomAttributes",
        "OriginalDocumentNumber"
    ]
    return process_data("purchaseorders", "PurchaseOrders", headers, last_id, use_timestamp=False)

@log_function_call
def process_products(last_id=0):
    headers = [
        "Code", "Name", "ProductGroupCode", "ProductGroupName", "Active", "Tag",
        "UnitPrice", "EAN", "Note", "ImageUrl", "MasterProduct", "PackagingCodes"
    ]
    return process_data("products", "Products", headers, last_id, use_timestamp=False)

@log_function_call
def process_forms(last_id=0):
    headers = [
        "FormID", "FormName", "ClientCode", "ClientName", "DateAndTime",
        "RepresentativeCode", "RepresentativeName", "StreetAddress", "ZIP",
        "ZIPExt", "City", "State", "Country", "Email", "Phone", "Mobile",
        "Territory", "Longitude", "Latitude", "SignatureURL", "VisitStart",
        "VisitEnd", "VisitID", "FormItems"
    ]
    return process_data("forms", "Forms", headers, last_id, use_timestamp=False)

@log_function_call
def process_photos(last_id=0):
    headers = [
        "PhotoID", "ClientCode", "ClientName", "Note", "DateAndTime", "PhotoURL",
        "RepresentativeCode", "RepresentativeName", "VisitID", "Tag"
    ]
    return process_data("photos", "Photos", headers, last_id, use_timestamp=False)

@log_function_call
def process_daily_working_time(last_id=0):
    headers = [
        "DailyWorkingTimeID", "Date", "DateAndTimeStart", "DateAndTimeEnd",
        "Length", "MileageStart", "MileageEnd", "MileageTotal", "LatitudeStart",
        "LongitudeStart", "LatitudeEnd", "LongitudeEnd", "RepresentativeCode",
        "RepresentativeName", "Note", "Tag", "NoOfVisits", "MinOfVisits",
        "MaxOfVisits", "MinMaxVisitsTime", "TimeAtClient", "TimeAtTravel"
    ]
    return process_data("dailyworkingtime", "DailyWorkingTime", headers, last_id, use_timestamp=False)

@log_function_call
def process_visit_schedules(last_id=None):
    headers = [
        "ScheduleDateAndTime", "RepresentativeCode", "RepresentativeName",
        "ClientCode", "ClientName", "StreetAddress", "ZIP", "ZIPExt", "City",
        "State", "Country", "Territory", "VisitNote", "DueDate"
    ]
    
    filename = "Repsly_VisitSchedules_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "VisitSchedules"
    ws.append(headers)

    end_date = datetime.now().strftime("%Y-%m-%d")
    begin_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/visitschedules/{begin_date}/{end_date}"
    
    data = fetch_data(url)
    row_count = 0
    
    if data and 'VisitSchedules' in data:
        for schedule in data['VisitSchedules']:
            ws.append([schedule.get(header) for header in headers])
            row_count += 1

    wb.save(filename)
    logging.info(f"Visit Schedules data saved to {filename}. Total rows: {row_count}")
    return filename, None

@log_function_call
def process_visit_realizations(last_id=None):
    headers = [
        "ScheduleId", "ProjectId", "EmployeeId", "EmployeeCode", "PlaceId",
        "PlaceCode", "ModifiedUTC", "TimeZone", "ScheduleNote", "Status",
        "DateTimeStart", "DateTimeStartUTC", "DateTimeEnd", "DateTimeEndUTC",
        "PlanDateTimeStart", "PlanDateTimeStartUTC", "PlanDateTimeEnd",
        "PlanDateTimeEndUTC", "Tasks"
    ]
    
    filename = "Repsly_VisitRealizations_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "VisitRealizations"
    ws.append(headers)

    modified_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    skip = 0
    row_count = 0

    while True:
        url = f"{BASE_URL}/visitrealizations"
        params = {'modified': modified_date, 'skip': skip}
        data = fetch_data(url, params)
        
        if data and 'VisitRealizations' in data:
            for visit in data['VisitRealizations']:
                ws.append([process_field(visit.get(header)) for header in headers])
                row_count += 1
            
            if len(data['VisitRealizations']) < 50:  
                break
            skip += 50
        else:
            break

    wb.save(filename)
    logging.info(f"Visit Realizations data saved to {filename}. Total rows: {row_count}")
    return filename, None

@log_function_call
def process_representatives(last_id=None):
    headers = [
        "Code", "Name", "Note", "Email", "Phone", "Territories", "Active",
        "Address1", "Address2", "City", "State", "ZipCode", "ZipCodeExt",
        "Country", "CountryCode", "Attributes"
    ]
    filename = "Repsly_Representatives_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Representatives"
    ws.append(headers)

    url = f"{BASE_URL}/representatives"
    data = fetch_data(url)
    row_count = 0

    if data and 'Representatives' in data:
        for rep in data['Representatives']:
            try:
                attributes = rep.get('Attributes', [])
                if attributes is None:
                    attributes = []
                attributes_str = ', '.join([f"{attr.get('Title', '')}:{attr.get('Type', '')}:{attr.get('Value', '')}" for attr in attributes])
                
                ws.append([
                    rep.get('Code'),
                    rep.get('Name'),
                    rep.get('Note'),
                    rep.get('Email'),
                    rep.get('Phone'),
                    ', '.join(rep.get('Territories', [])),
                    rep.get('Active'),
                    rep.get('Address1'),
                    rep.get('Address2'),
                    rep.get('City'),
                    rep.get('State'),
                    rep.get('ZipCode'),
                    rep.get('ZipCodeExt'),
                    rep.get('Country'),
                    rep.get('CountryCode'),
                    attributes_str
                ])
                row_count += 1
            except Exception as e:
                logging.error(f"Error processing representative: {rep.get('Code', 'Unknown')}. Error: {str(e)}")
    else:
        logging.warning("No 'Representatives' data found in the API response")

    wb.save(filename)
    logging.info(f"Representatives data saved to {filename}. Total rows: {row_count}")
    return filename, None

@log_function_call
def process_users(last_timestamp=0):
    headers = [
        "ID", "Code", "Name", "Email", "Active", "Role", "Note", "Phone",
        "Territories", "SendEmailEnabled", "Address1", "Address2", "City",
        "State", "ZipCode", "ZipCodeExt", "Country", "CountryCode",
        "Attributes", "Permissions"
    ]
    return process_data("users", "Users", headers, last_timestamp, use_timestamp=True)

@log_function_call
def process_document_types(last_id=None):
    headers = ["DocumentTypeID", "DocumentTypeName", "Statuses", "Pricelists"]
    filename = "Repsly_DocumentTypes_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "DocumentTypes"
    ws.append(headers)

    url = f"{BASE_URL}/documentTypes"
    data = fetch_data(url)
    row_count = 0
    
    if data and 'DocumentTypes' in data:
        for doc_type in data['DocumentTypes']:
            ws.append([
                doc_type.get('DocumentTypeID'),
                doc_type.get('DocumentTypeName'),
                ', '.join([status.get('DocumentStatusName', '') for status in doc_type.get('Statuses', [])]),
                ', '.join([pricelist.get('PricelistName', '') for pricelist in doc_type.get('Pricelists', [])])
            ])
            row_count += 1

    wb.save(filename)
    logging.info(f"Document Types data saved to {filename}. Total rows: {row_count}")
    return filename, None

@log_function_call
def process_pricelists(last_id=None):
    headers = ["ID", "Name", "IsDefault", "Active", "UsePrices"]
    filename = "Repsly_Pricelists_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Pricelists"
    ws.append(headers)

    url = f"{BASE_URL}/pricelists"
    data = fetch_data(url)
    row_count = 0
    
    if data and 'Pricelists' in data:
        for pricelist in data['Pricelists']:
            ws.append([
                pricelist.get('ID'),
                pricelist.get('Name'),
                pricelist.get('IsDefault'),
                pricelist.get('Active'),
                pricelist.get('UsePrices')
            ])
            row_count += 1

    wb.save(filename)
    logging.info(f"Pricelists data saved to {filename}. Total rows: {row_count}")
    return filename, None

@log_function_call
def process_pricelist_items(last_id=None):
    headers = [
        "PricelistID", "ID", "ProductID", "ProductCode", "Price", "Active",
        "ClientID", "ManufactureID", "DateAvailableFrom", "DateAvailableTo",
        "MinQuantity", "MaxQuantity"
    ]
    
    filename = "Repsly_PricelistItems_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "PricelistItems"
    ws.append(headers)

    row_count = 0
    pricelists_url = f"{BASE_URL}/pricelists"
    pricelists_data = fetch_data(pricelists_url)
    
    if pricelists_data and 'Pricelists' in pricelists_data:
        for pricelist in pricelists_data['Pricelists']:
            pricelist_id = pricelist.get('ID')
            if pricelist_id:
                url = f"{BASE_URL}/pricelistsItems/{pricelist_id}"
                data = fetch_data(url)
                
                if data and isinstance(data, list):
                    for item in data:
                        ws.append([
                            pricelist_id,
                            item.get('ID'),
                            item.get('ProductID'),
                            item.get('ProductCode'),
                            item.get('Price'),
                            item.get('Active'),
                            item.get('ClientID'),
                            item.get('ManufactureID'),
                            item.get('DateAvailableFrom'),
                            item.get('DateAvailableTo'),
                            item.get('MinQuantity'),
                            item.get('MaxQuantity')
                        ])
                        row_count += 1

    wb.save(filename)
    logging.info(f"Pricelist Items data saved to {filename}. Total rows: {row_count}")
    return filename, None

def create_combined_workbook(filenames):
    combined_wb = Workbook()
    combined_wb.remove(combined_wb.active)  # Remove the default sheet

    for filename in filenames:
        if os.path.exists(filename):
            try:
                wb = load_workbook(filename)
                for sheet_name in wb.sheetnames:
                    source_sheet = wb[sheet_name]
                    new_sheet = combined_wb.create_sheet(sheet_name)
                    
                    for row in source_sheet.iter_rows(values_only=True):
                        new_sheet.append(row)
                
                os.remove(filename)
            except Exception as e:
                logging.error(f"Error processing file {filename}: {str(e)}")
        else:
            logging.warning(f"File not found: {filename}")

    if not combined_wb.sheetnames:
        logging.warning("No data was combined. Creating a default sheet.")
        combined_wb.create_sheet("Empty")

    return combined_wb

def process_import_status(import_job_id):
    headers = [
        "ImportStatus", "RowsInserted", "RowsUpdated", "RowsInvalid", "RowsTotal",
        "Warnings", "Errors"
    ]
    
    filename = "Repsly_ImportStatus_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "ImportStatus"
    ws.append(headers)

    url = f"{BASE_URL}/importStatus/{import_job_id}"
    data = fetch_data(url)
    
    if data:
        warnings = '; '.join([f"{w['ItemID']}:{w['ItemName']}:{w['ItemStatus']}" for w in data.get('Warnings', [])])
        errors = '; '.join([f"{e['ItemID']}:{e['ItemName']}:{e['ItemStatus']}" for e in data.get('Errors', [])])
        
        ws.append([
            data.get('ImportStatus'),
            data.get('RowsInserted'),
            data.get('RowsUpdated'),
            data.get('RowsInvalid'),
            data.get('RowsTotal'),
            warnings,
            errors
        ])

    wb.save(filename)
    logging.info(f"Import Status data saved to {filename}")
    return filename

def process_endpoint(endpoint_func):
    start_time = time.time()
    filename = endpoint_func()
    end_time = time.time()
    print(f"Processing {filename} took {end_time - start_time:.2f} seconds")
    return filename

def create_combined_workbook(filenames):
    combined_wb = Workbook()
    combined_wb.remove(combined_wb.active)  # Remove the default sheet

    for filename in filenames:
        if os.path.exists(filename):
            try:
                wb = load_workbook(filename)
                for sheet_name in wb.sheetnames:
                    source_sheet = wb[sheet_name]
                    new_sheet = combined_wb.create_sheet(sheet_name)
                    
                    for row in source_sheet.iter_rows(values_only=True):
                        new_sheet.append(row)
                
                os.remove(filename)
            except Exception as e:
                logging.error(f"Error processing file {filename}: {str(e)}")
        else:
            logging.warning(f"File not found: {filename}")

    if not combined_wb.sheetnames:
        logging.warning("No data was combined. Creating a default sheet.")
        combined_wb.create_sheet("Empty")

    return combined_wb

def main(modules=None):
    last_ids = load_last_ids()
    filenames = []

    all_endpoints = {
        'representatives': process_representatives,
        'visitschedules': process_visit_schedules,
        'pricelistitems': process_pricelist_items,
        'pricelists': process_pricelists,
        'documenttypes': process_document_types,
        'purchaseorders': process_purchase_orders,
        'clients': process_clients,
        'clientnotes': process_client_notes,
        'visits': process_visits,
        'retailaudits': process_retail_audits,
        'products': process_products,
        'forms': process_forms,
        'photos': process_photos,
        'dailyworkingtime': process_daily_working_time,
        'visitrealizations': process_visit_realizations,
        'users': process_users,
    }

    if not modules:
        modules = all_endpoints.keys()

    logger.info("Starting Repsly data export...")

    def process_module(module):
        if module in all_endpoints:
            logger.info(f"Processing {module}...")
            process_func = all_endpoints[module]
            try:
                last_id = last_ids.get(module, 0)
                filename, new_last_id = process_func(last_id)
                if filename:
                    logger.info(f"{module.capitalize()} data exported successfully.")
                return module, filename, new_last_id
            except Exception as e:
                logger.error(f"Error processing {module}: {str(e)}", exc_info=True)
                return module, None, None
        else:
            logger.warning(f"Unknown module: {module}")
            return module, None, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_module = {executor.submit(process_module, module): module for module in modules}
        for future in concurrent.futures.as_completed(future_to_module):
            module, filename, new_last_id = future.result()
            if filename:
                filenames.append(filename)
            if new_last_id is not None:
                last_ids[module] = new_last_id

    try:
        combined_workbook = create_combined_workbook(filenames)
        if combined_workbook:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            combined_filename = f"Repsly_Export_Combined_{timestamp}.xlsx"
            combined_workbook.save(combined_filename)
            logger.info(f"Combined workbook saved as {combined_filename}")
        else:
            logger.error("Failed to create combined workbook.")
    except Exception as e:
        logger.error(f"Error creating or saving combined workbook: {str(e)}", exc_info=True)

    save_last_ids(last_ids)
    logger.info("Repsly data export completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Repsly data export modules.")
    parser.add_argument('modules', nargs='*', help='Modules to run. If none specified, all modules will run.')
    args = parser.parse_args()

    main(args.modules)
