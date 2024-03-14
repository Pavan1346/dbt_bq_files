from google.cloud import storage, bigquery
import os
import pandas as pd
import io

# Setting Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/User/Downloads/dbt-bq-414505-6e87b3af4e2d.json"

# Define project details
DATASET_ID = "dbt_bq_dataset_123"
BUCKET_NAME = "migration_project"
INPUT_FOLDER_NAME = 'input_file'
OUTPUT_FOLDER_NAME = 'output_file'

# Dictionary to store column names and widths for each file
file_columns = {
    "Sample-Superstore2.txt": {
        "cols": ['Row ID', 'Order ID', 'Order Date', 'Ship Date', 'Ship Mode', 'Customer ID', 'Customer Name', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region', 'Product ID', 'Category'],
        "widths": [10, 15, 12, 12, 15, 15, 22, 15, 15, 20, 20, 15, 10, 20, 15]
    },
    "normalfile.txt": {
        "cols": ['aa','bb','cc','dd'],
        "widths": [2,2,2,2]
    },
    "fixed_length.txt": {
         "cols": ['id','name','age','destination'],
        "widths": [4,8,4,11]
        
    }
}


# Process each line in the fixed-length file according to column info using pd.read_fwf
def process_fixed_length_file(file_content, cols, widths):
    # Create a StringIO object to simulate a file for pd.read_fwf
    file_obj = io.StringIO(file_content)
    
    # Reading the fixed-width file using pd.read_fwf with column widths
    df = pd.read_fwf(file_obj, widths=widths, names=cols)
    
    # Convert the dataframe to a list of dictionaries
    processed_lines = df.to_dict(orient='records')
    
    return processed_lines

# Upload to Google Cloud Storage
def upload_to_cloud_storage(bucket_name, output_folder, file_name, content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob = bucket.blob(f"{output_folder}/{file_name}".replace('.txt', '.csv'))
    try:
        destination_blob.upload_from_string(data=content, content_type='text/csv')
        print(f"File {file_name} loaded to {output_folder}.")
    except Exception as e:
        print(f"Error uploading file {file_name} to Cloud Storage: {e}")

# Load to BigQuery
def load_to_bigquery(bq_client, bucket_name, output_folder, file_name):
    file_url = f"gs://{bucket_name}/{output_folder}/{file_name}".replace('.txt', '.csv')
    table_name = file_name.split('.')[0]  # Use file name as table name
    table_id = f"{bq_client.project}.{DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    try:
        job = bq_client.load_table_from_uri(file_url, table_id, job_config=job_config)
        job.result()
        print(f"File {file_name} loaded to BigQuery table as {table_name}.")
    except Exception as e:
        print(f"Error loading file {file_name} to BigQuery table {table_id}: {e}")

# Main function to validate and process files
def validate_and_process_files():
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=f"{INPUT_FOLDER_NAME}/", delimiter='/')
    file_paths = [blob.name for blob in blobs if '.' in blob.name.split('/')[-1] and blob.name.endswith('.txt')]

    for file_path in file_paths:
        try:
            blob = bucket.blob(file_path)
            file_content = blob.download_as_string().decode("utf-8", errors="replace")
            file_name = file_path.split('/')[-1]

            if file_name in file_columns:
                cols = file_columns[file_name]["cols"]
                widths = file_columns[file_name]["widths"]

                processed_lines = process_fixed_length_file(file_content, cols, widths)
                
                csv_content = pd.DataFrame(processed_lines).to_csv(index=False,header=False)
                upload_to_cloud_storage(BUCKET_NAME, OUTPUT_FOLDER_NAME, file_name, csv_content)
                load_to_bigquery(bq_client, BUCKET_NAME, OUTPUT_FOLDER_NAME, file_name)
            else:
                print(f"Skipping file {file_name}. No column information found.")
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

if __name__ == "__main__":
    validate_and_process_files()
