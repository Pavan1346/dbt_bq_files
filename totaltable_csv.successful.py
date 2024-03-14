
from google.cloud import storage, bigquery
import csv
import io
import os
import tempfile
 
# Service account credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/User/Downloads/dbt-bq-414505-6e87b3af4e2d.json"
 
# Function to validate and process CSV files
def file_validation(input_folder_name, file_path, bucket_name, config_data, output_folder_name):
    # Initialize Google Cloud Storage client
    bucket = strg_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
 
    # Download CSV content with 'latin-1' encoding
    csv_content = blob.download_as_string().decode("latin-1")
    csv_file_like_object = io.StringIO(csv_content)
    csv_reader = csv.reader(csv_file_like_object)
 
    # Get the first row to determine the number of columns
    first_row = next(csv_reader)
    num_columns = len(first_row)
    print("Number of columns in the CSV file:", num_columns)
 
    # Extract file name from file path
    file_name = file_path.split('/')[-1]
 
    # Create a local temporary file to analyze the CSV
    local_file_path = os.path.join(tempfile.gettempdir(), file_name)
    blob = bucket.blob(f"{input_folder_name}/{file_name}")
    blob.download_to_filename(local_file_path)
 
    # Determine the delimiter of the CSV file
    try:
        with open(local_file_path, 'r', newline='') as file:
            dialect = csv.Sniffer().sniff(file.read(1024))
            delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ','  # Default delimiter
        # print("Could not determine delimiter. Using default delimiter ','.")
    print(f"The delimiter in the CSV file is: '{delimiter}'")
 
    # Extract configuration data for validation
    for config_dict in config_data:
        if config_dict['file_name'] == file_name:
            if int(config_dict["no_of_columns"]) == num_columns and config_dict["delimiter"] == delimiter:
                if config_dict['archive_flag'] == 'Y':
                    # Copy the validated file to the output folder
                    source_bucket = strg_client.bucket(bucket_name)
                    destination_bucket = strg_client.bucket(bucket_name)
                    source_blob = source_bucket.blob(file_path)
 
                    new_blob = source_bucket.copy_blob(
                        source_blob, destination_bucket, f"{output_folder_name}/{file_name}"
                    )
                    print(f"File Loaded to {output_folder_name}")
                else:
                    # Load the CSV file into BigQuery
                    file_url = config_dict['file_path']
                    job_config = bigquery.LoadJobConfig(
                        autodetect=True,
                        skip_leading_rows=1,
                        source_format=bigquery.SourceFormat.CSV,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                        ) #to avoid records same records entering to the table
                    table_name = config_dict['bq_table_name']
                    job = bq_client.load_table_from_uri(
                        file_url, f"{dataset_id}.{table_name}", job_config=job_config
                    )
                    job.result()
                    print("Loaded in BigQuery")
            else:
                print(f"{file_name} not matched with {config_table}")
 
# Function to extract configuration data from BigQuery
def extract_config_data():
    # Query the configuration table in BigQuery
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{config_table}`"
    query_job = bq_client.query(query)
    results = query_job.result()
 
    # Convert query results to a list of dictionaries
    config_data = []
    for row in results:
        row_dict = {}
        for field in results.schema:
            row_dict[field.name] = str(row[field.name])
        config_data.append(row_dict)
 
    return config_data
 
# Main function
def main():
    # Define input and output details
    bucket_name = "migration_project"
    input_folder_name = 'input_file'
    output_folder_name = 'output_file'
 
    # Get the list of CSV files in the input folder
    bucket = strg_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_folder_name + '/', delimiter='/')
    file_paths = [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]
 
    # Extract configuration data from BigQuery
    config_data = extract_config_data()
 
    # Validate and process each CSV file
    for file_path in file_paths:
        file_validation(input_folder_name, file_path, bucket_name, config_data, output_folder_name)
 
# Initialize Google Cloud Storage and BigQuery clients
strg_client = storage.Client()
bq_client = bigquery.Client()
 
# Project and dataset details
project_id = "dbt-bq-414505"
dataset_id = "dbt_bq_dataset_123"
config_table = "config_table"
 
# Execute the main function
if __name__ == "__main__":
    main()
 