import boto3
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO, BytesIO

# Configuration
S3_BUCKET = "lab2bucket-lnu"
FILE_NAME = "exchange_rates/exchange_rate_2022_01_12.csv"
PLOT_FILENAME = "uah_exchange_rates_2022.png"
DATA_OUTPUT_FILE = "exchange_rate_data_2022.csv"

def s3_download_file(bucket_name, file_name):
    """Download file from S3 into memory."""
    s3_client = boto3.client("s3")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = obj['Body'].read().decode('utf-8')
        return file_content
    except Exception as e:
        print(f"Error downloading file {file_name}: {e}")
        return None

def s3_upload_file_in_memory(file_buffer, bucket_name, file_name):
    """Upload file buffer to S3."""
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_fileobj(file_buffer, bucket_name, file_name)
        print(f"Файл {file_name} завантажено в S3 бакет {bucket_name}.")
    except Exception as e:
        print(f"Помилка завантаження {file_name}: {e}")

def plot_exchange_rates(file_name=FILE_NAME, bucket_name=S3_BUCKET):
    """Plot exchange rates from S3 data, output UI data, and upload the plot."""
    # Download and read CSV
    csv_data = s3_download_file(bucket_name, file_name)
    if csv_data is None:
        return
    
    try:
        df = pd.read_csv(StringIO(csv_data))
    except Exception as e:
        print(f"Error reading CSV data: {e}")
        return
    
    # Convert exchangedate to datetime
    try:
        df['exchangedate'] = pd.to_datetime(df['exchangedate'], format='%Y-%m-%d')
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        print("Sample dates in 'exchangedate' column:")
        print(df['exchangedate'].head())
        return
    
    # Format the data to match the plot's UI (daily exchange rates for USD and EUR)
    # Rename columns to match the plot's labels in Ukrainian
    ui_df = df.rename(columns={
        'exchangedate': 'Дата',
        'rate_USD': 'Курс USD (UAH)',
        'rate_EUR': 'Курс EUR (UAH)'
    })
    
    # Print the UI data as a table
    print("\nДані для графіка (Курс гривні до USD та EUR за 2022 рік):")
    print(ui_df.to_string(index=False, float_format="%.4f"))
    
    # Save the UI data to a new CSV file in memory
    csv_buffer = BytesIO()
    ui_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload the UI data CSV to S3
    s3_upload_file_in_memory(csv_buffer, bucket_name, f"exchange_rates/{DATA_OUTPUT_FILE}")
    csv_buffer.close()
    
    # Set plot style
    plt.style.use('grayscale')
    plt.figure(figsize=(12, 6))
    plt.gcf().set_facecolor('#BEBEBE')
    
    # Plot USD and EUR
    plt.plot(df['exchangedate'], df['rate_USD'], label='$ USD', color='green')
    plt.plot(df['exchangedate'], df['rate_EUR'], label='€ EUR', color='blue')
    
    # Customize plot to match the provided image
    plt.title('Курс гривні до USD та EUR за 2022 рік', fontsize=16, weight='bold')
    plt.xlabel('Дата', fontsize=14)
    plt.ylabel('Курс (UAH)', fontsize=14)
    plt.legend(title='Валюта', fontsize=12, title_fontsize='13', frameon=True, edgecolor='black')
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(range(int(df['rate_USD'].min()), int(df['rate_EUR'].max()) + 1, 1))
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    # Save plot to buffer
    plot_buffer = BytesIO()
    plt.savefig(plot_buffer, format='png')
    plot_buffer.seek(0)
    
    # Upload plot to S3
    s3_upload_file_in_memory(plot_buffer, bucket_name, f"plots/{PLOT_FILENAME}")
    
    # Close resources
    plt.close()
    plot_buffer.close()

# Run the plotting function
plot_exchange_rates()
