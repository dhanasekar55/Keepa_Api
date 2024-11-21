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
import json


API_ENDPOINT = "https://api.keepa.com/product?"
domain = 10  # Amazon India
API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
SERVICE_ACCOUNT_FILE = 'credencials.json' 
SPREADSHEET_ID = '1rW8Ff21XBR4TuO3DEWcaj8iib02RDJ_S0zGqOf8GdhE'  
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES) 
drive_service = build('drive', 'v3', credentials=credentials)



def save_to_google_sheets(product_data):
   
    headers = [
        'ASIN', 'TITLE', 'Categoryname','Price Type','CURRENT_PRICE', 'mrp_price',
        'AVERAGE_PRICE', 'discount2', 'short_url', 'graph_url', 
        'image_url', 'current_date'
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
        print(f"Error checking or adding headers: Retrying...")
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
        print(f"Error processing data in Google Sheets: Retrying...")
        time.sleep(20)

def get_param_keepa(asin):
    print(f"Fetching details for ASIN: {asin}")
    
    params = {
        'key': API_KEY,
        'domain': 10,  # Amazon India
        'asin': asin,
        'stats': 7,  # Retrieve price history and statistics
        'offers':20,
    }
    
    response = requests.get(API_ENDPOINT, params=params)
    
    if response.status_code == 200:
        product_data = response.json()
        # Check if the response contains product data
        if 'products' in product_data and product_data['products']:
            product = product_data['products'][0]  
           
            avg = product['stats']['avg'][1]  # Avg price
            current = product['stats']['current'][1]  # Current price
            mrp = product['stats']['current'][4]
            rootCategorys = product['rootCategory']
            
            price_inr_current = current / 100
            price_inr_avg = avg / 100
            mrp_price=mrp/100
            price_type=44
            
            # discount2=round((price_inr_avg-price_inr_current)/price_inr_avg*100)
            Coupons = product['coupon'][0] if product.get('coupon') and len(product['coupon']) > 0 else None
            if Coupons is not None:
                    discount2 = Coupons * -1
            else:
                discount2 = "N/A"

            title = product['title']
            images = product.get('imagesCSV', '')
            image_url = f"https://images-na.ssl-images-amazon.com/images/I/{images.split(',')[0]}" if images else 'N/A'
            short_url = f"https://www.amazon.in/dp/{asin}?tag=gadgetmart4444-21"
            graph = f'https://api.keepa.com/graphimage?key={API_KEY}&domain=10&asin={asin}&range=31'
            
            asin = product['asin']
            
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
            Categoryname = category_map.get(rootCategorys, "none")
            try:
                response = requests.get(graph)
            except:
                print("Retrying...")
                time.sleep(5) 
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
                    pass
                
                product_data = [
                    asin, title,Categoryname,price_type,price_inr_current,mrp_price, price_inr_avg,discount2, short_url, graph_url, image_url
                ]
                image_url_value = product_data[10]
                asin_value = product_data[0]
                discount2_value = product_data[7]
                price_inr_current_value =product_data[4]
                mrp_price_value=product_data[5]
                price_inr_avg_value=product_data[6]
                
                exclude_key_word = ["Women"]
                        
                if not any(word in title for word in exclude_key_word):
                    if price_inr_current_value >=-0.001:
                        if mrp_price_value >=-0.001:
                            if price_inr_avg_value >=-0.001:
                                if mrp_price_value > price_inr_avg_value:
                                    if image_url_value!=None:
                                        if discount2_value!=None:
                                            if all(product_data):  # Ensure no value is missing
                                                save_to_google_sheets(product_data)
                                            else:
                                                print(f"Skipping ASIN {asin} due to missing data")
                                        else:
                                            print(f"Skipping ASIN {asin_value} due to Coupon None")   
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
                    print(f"Keyword found, skipping ASIN {asin_value} due to keyword in title.")        
                        
            else:
                print("Error:", response.status_code, response.json())
                print("Please Wait 30sec")
                time.sleep(30)

def fetch_asin_list_with_pagination():
    page = 0
    has_more_data = True
    
    while has_more_data:
        params = {
            "couponOneTimePercent_gte": 20,
            "couponOneTimePercent_lte": 80,
            "sort": [["current_SALES", "asc"]],
            "lastOffersUpdate_gte": 7251131,
            "productType": [0, 1, 2],
            "page": page,
            "perPage": 150
        }
        
        selection = json.dumps(params)
        url = f"https://api.keepa.com/query?key={API_KEY}&domain={domain}&selection={selection}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            if 'asinList' in data and data['asinList']:
                asin_list = data['asinList']
                # print(f"ASIN List fetched for page {page}: {asin_list}")
                
                # Loop through the ASIN list and pass each ASIN to get_param_keepa
                for asin in asin_list:
                    get_param_keepa(asin)
                
                # Increment page for next fetch
                page += 1
            else:
                # No more ASINs found, stop pagination
                has_more_data = False
                print("No more ASINs found, stopping pagination.")
        else:
            print(f"Failed to fetch data on page {page}. Status code: {response.status_code}")
            time.sleep(30)

fetch_asin_list_with_pagination()


