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


import base64
import functools
import json
import logging
import os
import requests
import time

from datetime import datetime, timedelta
from openpyxl import Workbook, load_workbook


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

##############################################################################################################
# These should be in environment variables if deploying this for prod, but we're just testing at the moment ##
                                                                                                             #
API_USERNAME = "<API KEY USER>"                                                                              #
API_PASSWORD = "<API KEY PASS>"                                                                              #
BASE_URL = "https://api.repsly.com/v3/export"                                                                #
                                                                                                             #
##############################################################################################################


# Repsly loves their *basic* base64 auth ...
auth_header = base64.b64encode(f"{API_USERNAME}:{API_PASSWORD}".encode()).decode()
headers = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/json"
}

# Use @log_function_call for logging
def log_function_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logging.debug(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.debug(f"Finished {func.__name__}. Execution time: {end_time - start_time:.2f} seconds")
        
        if func.__name__ == 'fetch_data':
            if result:
                try:
                    data_str = json.dumps(result, indent=2)[:1000]
                    logging.debug(f"API Response Data (truncated to 1000 chars):\n{data_str}")
                    
                    if isinstance(result, dict):
                        for key, value in result.items():
                            if isinstance(value, list):
                                logging.debug(f"'{key}' contains {len(value)} items")
                            elif isinstance(value, dict):
                                logging.debug(f"'{key}' is a dictionary with {len(value)} key-value pairs")
                    elif isinstance(result, list):
                        logging.debug(f"Response is a list with {len(result)} items")
                except Exception as e:
                    logging.error(f"Error parsing API response: {str(e)}")
            else:
                logging.warning("API response is empty or None")
        
        if isinstance(result, str) and result.endswith('.xlsx'):
            try:
                from openpyxl import load_workbook
                wb = load_workbook(result)
                for sheet_name in wb.sheetnames:
                    sheet = wb[sheet_name]
                    logging.debug(f"Sheet '{sheet_name}' in {result} has {sheet.max_row} rows and {sheet.max_column} columns")
                    if sheet.max_row > 1:
                        first_data_row = list(sheet.iter_rows(min_row=2, max_row=2, values_only=True))[0]
                        logging.debug(f"First data row in '{sheet_name}': {first_data_row}")
                    else:
                        logging.warning(f"Sheet '{sheet_name}' in {result} has no data rows")
            except Exception as e:
                logging.error(f"Error analyzing {result}: {str(e)}")
        return result
    return wrapper

def save_last_ids(workbook, last_ids):
    if 'LastIDs' in workbook.sheetnames:
        sheet = workbook['LastIDs']
        workbook.remove(sheet)
    
    sheet = workbook.create_sheet('LastIDs')
    sheet.append(['Endpoint', 'Last ID/Timestamp'])
    
    for endpoint, last_id in last_ids.items():
        sheet.append([endpoint, last_id])

## We use this as a template form to process each endpoint. We reference the MetaCollectionResult and then look for the next key

def process_data(endpoint, key_name, headers):
    filename = f"Repsly_{key_name}_Export.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = key_name

    ws.append(headers)

    row_count = 0
    last_id = 0
    while True:
        url = f"{BASE_URL}/{endpoint}/{last_id}"
        data = fetch_data(url)

        if data and key_name in data:
            for item in data[key_name]:
                row = [process_field(item.get(header)) for header in headers]
                ws.append(row)
                row_count += 1

            meta = data.get('MetaCollectionResult', {})
            new_last_id = meta.get('LastID') or meta.get('LastTimeStamp')
            if new_last_id:
                last_id = new_last_id
            else:
                break
        else:
            logging.warning(f"No '{key_name}' found in data from {url}")
            break

    wb.save(filename)
    logging.info(f"{key_name} data saved to {filename}. Total rows: {row_count}")
    return filename, last_id

def fetch_data(url, params=None):
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {url}: {response.status_code}")
        return None


def process_field(value):
    if isinstance(value, list):
        return ', '.join(str(v) for v in value)
    elif isinstance(value, dict):
        return ', '.join(f"{k}:{v}" for k, v in value.items())
    return value

##############################################################################################################
#       These next functions are the API endpoint headers as described here:                                 #
#             https://repsly-dev.readme.io/reference/getting-started-1                                       #
##############################################################################################################

def process_clients():
    headers = [
        "ClientID", "TimeStamp", "Code", "Name", "Active", "Tag", "Territory",
        "RepresentativeCode", "RepresentativeName", "StreetAddress", "ZIP", "City",
        "State", "Country", "Email", "Phone", "Mobile", "Website", "ContactName",
        "ContactTitle", "Note", "Status", "CustomFields", "PriceLists", "AccountCode"
    ]
    return process_data("clients", "Clients", headers)

def process_client_notes():
    headers = [
        "ClientNoteID", "TimeStamp", "DateAndTime", "RepresentativeCode",
        "RepresentativeName", "ClientCode", "ClientName", "StreetAddress",
        "ZIP", "ZIPExt", "City", "State", "Country", "Email", "Phone",
        "Mobile", "Territory", "Longitude", "Latitude", "Note", "VisitID"
    ]
    return process_data("clientnotes", "ClientNotes", headers)

def process_visits():
    headers = [
        "VisitID", "TimeStamp", "Date", "RepresentativeCode", "RepresentativeName",
        "ExplicitCheckIn", "DateAndTimeStart", "DateAndTimeEnd", "ClientCode",
        "ClientName", "StreetAddress", "ZIP", "ZIPExt", "City", "State", "Country",
        "Territory", "LatitudeStart", "LongitudeStart", "LatitudeEnd", "LongitudeEnd",
        "PrecisionStart", "PrecisionEnd", "VisitStatusBySchedule", "VisitEnded"
    ]
    return process_data("visits", "Visits", headers)

def process_retail_audits():
    headers = [
        "RetailAuditID", "RetailAuditName", "Cancelled", "ClientCode", "ClientName",
        "DateAndTime", "RepresentativeCode", "RepresentativeName", "ProductGroupCode",
        "ProductGroupName", "ProductCode", "ProductName", "Present", "Price",
        "Promotion", "ShelfShare", "ShelfSharePercent", "SoldOut", "Stock",
        "CustomFields", "Note", "VisitID"
    ]
    return process_data("retailaudits", "RetailAudits", headers)

def process_purchase_orders():
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
    return process_data("purchaseorders", "PurchaseOrders", headers)

def process_products():
    headers = [
        "Code", "Name", "ProductGroupCode", "ProductGroupName", "Active", "Tag",
        "UnitPrice", "EAN", "Note", "ImageUrl", "MasterProduct", "PackagingCodes"
    ]
    return process_data("products", "Products", headers)

def process_forms():
    headers = [
        "FormID", "FormName", "ClientCode", "ClientName", "DateAndTime",
        "RepresentativeCode", "RepresentativeName", "StreetAddress", "ZIP",
        "ZIPExt", "City", "State", "Country", "Email", "Phone", "Mobile",
        "Territory", "Longitude", "Latitude", "SignatureURL", "VisitStart",
        "VisitEnd", "VisitID", "FormItems"
    ]
    return process_data("forms", "Forms", headers)

def process_photos():
    headers = [
        "PhotoID", "ClientCode", "ClientName", "Note", "DateAndTime", "PhotoURL",
        "RepresentativeCode", "RepresentativeName", "VisitID", "Tag"
    ]
    return process_data("photos", "Photos", headers)

def process_daily_working_time():
    headers = [
        "DailyWorkingTimeID", "Date", "DateAndTimeStart", "DateAndTimeEnd",
        "Length", "MileageStart", "MileageEnd", "MileageTotal", "LatitudeStart",
        "LongitudeStart", "LatitudeEnd", "LongitudeEnd", "RepresentativeCode",
        "RepresentativeName", "Note", "Tag", "NoOfVisits", "MinOfVisits",
        "MaxOfVisits", "MinMaxVisitsTime", "TimeAtClient", "TimeAtTravel"
    ]
    return process_data("dailyworkingtime", "DailyWorkingTime", headers)

##############################################################################################################
#           Because of how Repsly tracks these (using a pair of timestamps...) we have to write these        #
#               as bespoke functions for each endpoint...                                                    #
##############################################################################################################

def process_visit_schedules():
    headers = [
        "ScheduleDateAndTime", "RepresentativeCode", "RepresentativeName",
        "ClientCode", "ClientName", "StreetAddress", "ZIP", "ZIPExt", "City",
        "State", "Country", "Territory", "VisitNote", "DueDate"
    ]
    
    # This endpoint requires date range, so we need a custom implementation
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
            ws.append([process_field(schedule.get(header)) for header in headers])
            row_count += 1

    wb.save(filename)
    logging.info(f"Visit Schedules data saved to {filename}. Total rows: {row_count}")
    return filename

def process_visit_realizations():
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
    return filename

def process_representatives():
    headers = [
        "Code", "Name", "Note", "Email", "Phone", "Territories", "Active",
        "Address1", "Address2", "City", "State", "ZipCode", "ZipCodeExt",
        "Country", "CountryCode", "Attributes"
    ]
    return process_data("representatives", "Representatives", headers)

def process_users():
    headers = [
        "ID", "Code", "Name", "Email", "Active", "Role", "Note", "Phone",
        "Territories", "SendEmailEnabled", "Address1", "Address2", "City",
        "State", "ZipCode", "ZipCodeExt", "Country", "CountryCode",
        "Attributes", "Permissions"
    ]
    return process_data("users", "Users", headers)


def process_document_types():
    headers = ["DocumentTypeID", "DocumentTypeName", "Statuses", "Pricelists"]
    return process_data("documentTypes", "DocumentTypes", headers)

def process_pricelists():
    headers = ["ID", "Name", "IsDefault", "Active", "UsePrices"]
    return process_data("pricelists", "Pricelists", headers)

def process_pricelist_items():
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
    pricelists_data = fetch_data(f"{BASE_URL}/pricelists")
    
    if pricelists_data and 'Pricelists' in pricelists_data:
        for pricelist in pricelists_data['Pricelists']:
            pricelist_id = pricelist.get('ID')
            if pricelist_id:
                url = f"{BASE_URL}/pricelistsItems/{pricelist_id}"
                data = fetch_data(url)
                
                if data and isinstance(data, list):
                    for item in data:
                        ws.append([process_field(item.get(header)) for header in headers])
                        row_count += 1

    wb.save(filename)
    logging.info(f"Pricelist Items data saved to {filename}. Total rows: {row_count}")
    return filename


    
def process_endpoint(endpoint_func):
    start_time = time.time()
    filename = endpoint_func()
    end_time = time.time()
    print(f"Processing {filename} took {end_time - start_time:.2f} seconds")
    return filename


##############################################################################################################
#                 Though we don't really use this one yet, it's included for completeness.                   #
##############################################################################################################

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

##############################################################################################################
#                       A later revision of this will have us using one workbook from the start...           #
#                           but for testing this is good enough. We're saving each to their own workbook     #
#                              and then combining them at the end. It's ineffecient, I know.                 #
##############################################################################################################

def create_combined_workbook(filenames):
    combined_wb = Workbook()
    combined_wb.remove(combined_wb.active)  # Remove the default sheet

    for filename in filenames:
        if filename and os.path.exists(filename):
            wb = load_workbook(filename)
            for sheet_name in wb.sheetnames:
                source_sheet = wb[sheet_name]
                new_sheet = combined_wb.create_sheet(sheet_name)
                
                for row in source_sheet.iter_rows(values_only=True):
                    new_sheet.append(row)
            
            # Optionally, remove the individual file after combining
            os.remove(filename)
        else:
            print(f"Warning: File not found or invalid filename: {filename}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"Repsly_Export_Combined_{timestamp}.xlsx"
    combined_wb.save(combined_filename)
    print(f"Combined workbook saved as {combined_filename}")

def main():
    last_ids = {}
    filenames = []

    endpoints = [
        ('clients', process_clients),
        ('clientnotes', process_client_notes),
        ('visits', process_visits),
        ('retailaudits', process_retail_audits),
        ('purchaseorders', process_purchase_orders),
        ('documentTypes', process_document_types),
        ('products', process_products),
        ('pricelists', process_pricelists),
        ('pricelistItems', process_pricelist_items),
        ('forms', process_forms),
        ('photos', process_photos),
        ('dailyworkingtime', process_daily_working_time),
        ('visitschedules', process_visit_schedules),
        ('visitrealizations', process_visit_realizations),
        ('representatives', process_representatives),
        ('users', process_users),
    ]

    for endpoint, process_func in endpoints:
        filename, last_id = process_func()
        filenames.append(filename)
        last_ids[endpoint] = last_id

    import_job_id = None  
    if import_job_id:
        filename, _ = process_import_status(import_job_id)
        filenames.append(filename)

    combined_workbook = create_combined_workbook(filenames)
    save_last_ids(combined_workbook, last_ids)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"Repsly_Export_Combined_{timestamp}.xlsx"
    combined_workbook.save(combined_filename)
    logging.info(f"Combined workbook with LastIDs saved as {combined_filename}")

if __name__ == "__main__":
    main()
