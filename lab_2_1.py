import requests
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

# Configuration
S3_BUCKET = "lab2bucket-lnu"

def s3_upload_file_in_memory(file_buffer, bucket_name, file_name):
    """Upload file buffer to S3."""
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_fileobj(file_buffer, bucket_name, file_name)
        print(f"Файл {file_name} завантажено в S3 бакет {bucket_name}.")
    except Exception as e:
        print(f"Помилка завантаження {file_name}: {e}")

def fetch_exchange_rates(start_year, start_month=1, end_month=12, bucket_name=S3_BUCKET):
    """Fetch exchange rates for a given year and upload to S3."""
    if not (1 <= start_month <= 12) or not (1 <= end_month <= 12):
        print("Задайте місяць цифрою від 1 до 12.")
        return
    
    # Form start and end dates in YYYYMMDD format
    start_date = f"{start_year}{str(start_month).zfill(2)}01"
    end_day = pd.Timestamp(year=start_year, month=end_month, day=1).days_in_month
    end_date = f"{start_year}{str(end_month).zfill(2)}{str(end_day).zfill(2)}"
    
    # API URLs for USD and EUR
    url_usd = f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={start_date}&end={end_date}&valcode=USD&sort=exchangedate&json"
    url_eur = f"https://bank.gov.ua/NBU_Exchange/exchange_site?start={start_date}&end={end_date}&valcode=EUR&sort=exchangedate&json"
    
    # Fetch data
    try:
        response_usd = requests.get(url_usd)
        response_eur = requests.get(url_eur)
        if response_usd.status_code == 200 and response_eur.status_code == 200:
            data_usd = response_usd.json()
            data_eur = response_eur.json()
        else:
            print("Помилка отримання даних від API")
            return
    except Exception as e:
        print(f"Помилка запиту до API: {e}")
        return
    
    # Convert to DataFrames
    df_usd = pd.DataFrame(data_usd)
    df_eur = pd.DataFrame(data_eur)
    
    # Merge DataFrames on exchangedate
    if not df_usd.empty and not df_eur.empty:
        df_combined = pd.merge(
            df_usd[['exchangedate', 'rate']],
            df_eur[['exchangedate', 'rate']],
            on='exchangedate',
            suffixes=('_USD', '_EUR')
        )
    else:
        print("Дані для одного з валют відсутні.")
        return
    
    # Save to CSV in memory
    file_name = f"exchange_rate_{start_year}_{str(start_month).zfill(2)}_{str(end_month).zfill(2)}.csv"
    csv_buffer = BytesIO()
    df_combined.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload to S3
    s3_upload_file_in_memory(csv_buffer, bucket_name, f"exchange_rates/{file_name}")
    csv_buffer.close()

# Fetch data for 2022 (January to December)
fetch_exchange_rates(2022, 1, 12)
