import requests
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import pytz
import sys
import re
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
from googleapiclient.http import MediaIoBaseUpload
from PIL import Image
import numpy as np
import io
import random

def current_time_in_delhi_to_milliseconds():
    delhi_tz = pytz.timezone('Asia/Kolkata')
    delhi_datetime = datetime.now(delhi_tz)
    timestamp_milliseconds = int(delhi_datetime.timestamp() * 1000)
    return timestamp_milliseconds

API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
SERVICE_ACCOUNT_FILE = 'credencials.json' 
SPREADSHEET_ID = '1rW8Ff21XBR4TuO3DEWcaj8iib02RDJ_S0zGqOf8GdhE'  
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES) 
drive_service = build('drive', 'v3', credentials=credentials)


service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
# Get current timestamp in milliseconds (New Delhi)
timestamp = current_time_in_delhi_to_milliseconds()
start_timestamp = 0
end_timestamp = int(timestamp / 1000)


from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from datetime import datetime, timedelta

def save_to_google_sheets(product_data):
    headers = [
        'ASIN', 'TITLE', 'Categoryname', 'priceTypes', 'CURRENT_PRICE', 'mrp_price',
        'AVERAGE_PRICE', 'discount2', 'short_url', 'graph_url', 
        'full_image_path', 'current_date'
    ]

    value_input_option = "RAW"
    asin_to_update = product_data[0]  # ASIN is expected to be the first element in product_data

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    try:
        # Check and add headers if they do not exist
        existing_values = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Sheet1!A1:L1").execute().get('values', [])
        if not existing_values:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range="Sheet1!A1",
                valueInputOption='RAW',
                body={'values': [headers]}
            ).execute()
            print("Headers added to the first row.")
    except HttpError as err:
        # print(f"Error checking or adding headers: Retrying...")
        time.sleep(20)

    try:
        existing_data = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="Sheet1!A2:L").execute().get('values', [])
        current_date = datetime.now() 
        product_data.append(current_date.strftime("%Y-%m-%d")) 

        values_to_add = []  
        values_to_update = []  
        row_to_update = None  

        # Identify rows to delete based on date
        rows_to_delete = []
        for idx, row in enumerate(existing_data, start=2):  # Start from row 2 since row 1 is the header
            if row:
                saved_date = row[-1] 
                try:
                    saved_date_obj = datetime.strptime(saved_date, "%Y-%m-%d") 
                    if current_date - saved_date_obj > timedelta(days=7):  # Check if older than 7 days
                        rows_to_delete.append(idx) 
                except ValueError:
                    print(f"Invalid date format in row {idx}: {saved_date}")

           
            if row and row[0] == asin_to_update:  # Assuming ASIN is in column A (index 0)
                values_to_update.append((idx, product_data))
                row_to_update = idx

        # Delete old rows if any
        if rows_to_delete:
            print(f"Deleting {len(rows_to_delete)} rows older than 7 days.")
            for row_number in sorted(rows_to_delete, reverse=True):
                try:
                    sheet.values().clear(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"Sheet1!A{row_number}:O{row_number}"
                    ).execute()
                    print(f"Deleted row {row_number}.")
                except HttpError as e:
                    print(f"Error deleting row {row_number}: Retrying..")
                    time.sleep(20)
                    
        
        if row_to_update is None:
            values_to_add.append(product_data)

        # Save new ASINs to Google Sheets
        if values_to_add:
            try:
                sheet.values().append(
                    spreadsheetId=SPREADSHEET_ID,
                    range="Sheet1!A2",  # Append starting from row 2
                    valueInputOption=value_input_option,
                    body={'values': values_to_add}
                ).execute()
                print(f"Data saved successfully for {len(values_to_add)} new ASINs.")
            except HttpError as e:
                print(f"Error saving new data to Google Sheets: Retrying...")
                time.sleep(20)

        # Update existing products
        for row_number, product_values in values_to_update:
            try:
                sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"Sheet1!A{row_number}:L{row_number}",  # Update the specific row
                    valueInputOption=value_input_option,
                    body={'values': [product_values]}
                ).execute()
                print(f"Data updated successfully for ASIN {product_values[0]} at row {row_number}.")
            except HttpError as e:
                print(f"Error updating ASIN {product_values[0]}: {e}")

    except HttpError as err:
        # print(f"Error processing data in Google Sheets: retrying")
        time.sleep(20)

def fetch_asins_from_keepa():
    url = f"https://api.keepa.com/deal?key={API_KEY}"
    current_page = 0
    # max_pages = 1
    priceTypes=18
    
    while True:
       
        payload = {
            "page": current_page,
            "domainId": "10",  # Amazon India
            "excludeCategories": [976445031,1571277031,976416031,6648217031,9223372036854775807,126,83,64,1389409031],
            "includeCategories": [],  # Category IDs
            "priceTypes": priceTypes,
            "deltaRange": [start_timestamp, end_timestamp],
            "deltaPercentRange": [10, 80],  # Discount range from 10% to 80%
            "salesRankRange": [-1, -1],
            "currentRange": [40700, 2147483647],  # Current price range (in cents)
            "minRating": -1,
            "isLowest": False,
            "isLowest90": False,
            "isLowestOffer": False,
            "isOutOfStock": False,
            "titleSearch": "",
            "isRangeEnabled": True,
            "isFilterEnabled": False,
            "filterErotic": True,
            "singleVariation": True,
            "hasReviews": False,
            "isPrimeExclusive": False,
            "mustHaveAmazonOffer": False,
            "mustNotHaveAmazonOffer": False,
            "sortType": 1,
            "dateRange": "0",
            "warehouseConditions": [1, 2, 3, 4, 5]  # Warehouse conditions for used products
        }
        
        response = requests.post(url, json=payload)

        if response.status_code == 200:
            response_data = response.json()
            dr_products = response_data.get('deals', [])
            get_dr = dr_products.get('dr', [])

            if not isinstance(get_dr, list):
                print("Unexpected format for 'dr':", type(get_dr))
                break
            else:
                total_products = len(get_dr)
                print(f"Total products found on page {current_page + 1}: {total_products}")

                if total_products == 0:
                    print("No products found. Exiting.")
                    break

                for product in get_dr:
                    if isinstance(product, dict):
                        asin = product.get('asin', 'N/A')
                        title = product.get('title', '')
                        rootcat = product.get('rootCat', '')
                        image = product.get('image', None)
                        current = product.get('current', '')[1]
                        mrp = product.get('current', '')[4]
                        discount2 =product.get('deltaPercent','')[0][1]
                       
                        
                        avg = product.get('avg', '')[1][1]
                        short_url = f"https://www.amazon.in/dp/{asin}?tag=gadgetmart4444-21"
                        graph = f'https://api.keepa.com/graphimage?key={API_KEY}&domain=10&asin={asin}&range=31'

                    
                        if image:
                            try:
                                filename = ''.join(chr(i) for i in image)
                                full_image_path = f"https://images-na.ssl-images-amazon.com/images/I/{filename}"
                            except TypeError:
                                print(f"Invalid image data for ASIN {asin}")
                                full_image_path = None
                        else:
                            print(f"No image found for ASIN {asin}")
                            full_image_path = None

                        price_inr_current = current / 100
                        price_inr_avg = avg / 100
                        mrp_price=mrp/100
                        # discount2=round((price_inr_avg-price_inr_current)/price_inr_avg*100)
                        category_map = {
                            976419031: "Electronics",
                            976392031: "Computers & Accessories",
                            1571271031: "Clothing & Accessories",
                            1350380031: "Toys & Games",
                            976442031: "Home & Kitchen",
                            2454169031:"Bags, Wallets and Luggage",
                            1571274031:"Baby",
                            3704992031:"Home Improvement",
                            976460031:"Video Games",
                            4772060031:"Car & Motorbike",
                            1951048031:"Jewellery",
                            976389031:"Books",
                            1355016031:"Beauty",
                            1984443031:"Sports, Fitness & Outdoors",
                            2454181031:"Pet Supplies",
                            1350384031:"Health & Personal Care",
                            1350387031:"Watches",
                            5866078031:"Industrial & Scientific",
                            1571283031:"Shoes & Handbags",
                            2454175031:"Outdoor Living",
                            3677697031:"Musical Instruments",
                            2454172031:"Office Products",
                            1571277031:"Kindle Store",
                            6648217031:"Fashion",
                            976445031:"Music",
                            976451031:"Software",
                            2454178031:"Grocery & Gourmet Foods",
                            976416031:"Movies & TV Shows"
                        }
                        Categoryname = category_map.get(rootcat, "none")
                        try:
                            response = requests.get(graph)
                        except:
                             print("Retrying...")
                             time.sleep(5)
                             continue 
                        if response.status_code == 200:
                            try:
                            # Step 1: Open and modify the image
                                img = Image.open(io.BytesIO(response.content)).convert('RGB')
                                img_arr = np.array(img)
                                img_arr[140:200, 400:500] = np.clip((255, 255, 255), 0, 255)
                                modified_img = Image.fromarray(img_arr)

                                # Step 2: Save the modified image to a BytesIO object
                                img_byte_arr = io.BytesIO()
                                modified_img.save(img_byte_arr, format='JPEG')
                                img_byte_arr.seek(0)

                                # Step 3: Attempt to upload the modified image to Google Drive
                                file_metadata = {'name': 'modified_image.jpg', 'mimeType': 'image/jpeg'}
                                media = MediaIoBaseUpload(img_byte_arr, mimetype='image/jpeg')

                                try:
                                    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                                    file_id = file.get('id')

                                    # Attempt to create permissions for the file to be publicly accessible
                                    drive_service.permissions().create(
                                        fileId=file_id,
                                        body={'type': 'anyone', 'role': 'reader'}
                                    ).execute()

                                    graph_url = f"https://drive.google.com/uc?id={file_id}"
                                    # print(f"Image uploaded successfully: {graph_url}")

                                except HttpError as e:
                                    print(f"Error uploading file to Google Drive: {e}")
                                    pass 

                            except Exception as e:
                                print(f"Unexpected error during image processing or uploading: {e}")
                                continue
                            
                
                        product_data = [
                            asin, title,Categoryname,priceTypes, price_inr_current,mrp_price, price_inr_avg,discount2, short_url, graph_url, full_image_path
                        ]
                        full_image_path_value = product_data[10]
                        asin_value = product_data[0]
                        discount2_value = product_data[7]
                        price_inr_current_value =product_data[4]
                        mrp_price_value=product_data[5]
                        price_inr_avg_value=product_data[6]
                        
                        
                        exclude_key_word = ["Women"]
                        
                        if not any(word in title for word in exclude_key_word):
                            if discount2_value > 10:
                                if price_inr_current_value >=-0.001:
                                    if mrp_price_value >=-0.001:
                                        if price_inr_avg_value >=-0.001:
                                            if mrp_price_value > price_inr_avg_value:
                                                if full_image_path_value!=None:
                                                    if all(product_data):  # Ensure no value is missing
                                                        save_to_google_sheets(product_data)
                                                    else:
                                                        print(f"Skipping ASIN {asin} due to missing data")
                                                else:
                                                    print(f"Skipping ASIN {asin_value} due to Image None")
                                            else:
                                                print(f"Skipping ASIN {asin_value} due to average value is greater than mrp : average - {price_inr_avg_value},mrp - {mrp_price_value}")
                                        else:
                                            print(f"Skipping ASIN {asin_value} due to average value: {price_inr_avg_value}")
                                    else:
                                        print(f"Skipping ASIN {asin_value} due to MRP value: {mrp_price_value}")
                                else:
                                    print(f"Skipping ASIN {asin_value} due to current value: {price_inr_current_value}")
                            else:
                                print(f"Skipping ASIN {asin_value} due to discount2 value: {discount2_value}")                       
                        else:
                            print(f"Keyword found, skipping ASIN {asin_value} due to keyword in title.")

                current_page += 1  # Move to the next page
        else:
            print("Error:", response.status_code, response.json())
            time.sleep(30)

fetch_asins_from_keepa()
