from google.cloud import storage, bigquery
import json
import os
import csv
import io

# Setting our Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/User/Downloads/dbt-bq-414505-6e87b3af4e2d.json"

# Define project and dataset details
PROJECT_ID = "dbt-bq-414505"
DATASET_ID = "dbt_bq_dataset_123"
CONFIG_TABLE = "config_fixedlength3"
BUCKET_NAME = "migration_project"
INPUT_FOLDER_NAME = 'input_file'
OUTPUT_FOLDER_NAME = 'output_file'

# Parse JSON data string and add starting position to each field
def parse_json_with_starting_position(json_data_string):
    config_data_dict = json.loads(json_data_string)
    starting_position = 1
    for field_info in config_data_dict.values():
        field_info['starting_position'] = starting_position
        starting_position += field_info['number_of_positions']
    return config_data_dict

# converting fixedlength files to csv format
def fixed_length_file_to_csv(processed_lines, column_info):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=column_info.keys())
    writer.writeheader()
    for line in processed_lines:
        writer.writerow(line)
    return output.getvalue()

# Process each line in the fixed-length file according to column info.
def process_fixed_length_file(file_content, column_info):
    processed_lines = []
    record_length = max(field['starting_position'] + field['number_of_positions'] - 1 for field in column_info.values())
    for line in file_content.splitlines():
        # if len(line) == record_length:
        record = {}
        for field_name, field_info in column_info.items():
            start_pos = field_info['starting_position'] - 1
            length = field_info['number_of_positions']
            record[field_name] = line[start_pos:start_pos + length].strip()
        processed_lines.append(record)
    return processed_lines

# when the file matches with config table it moves to ouput _folder
def upload_to_cloud_storage(bucket_name, output_folder, file_name, content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob = bucket.blob(f"{output_folder}/{file_name}".replace('.prn', '.csv'))
    destination_blob.upload_from_string(data=content, content_type='text/csv')
    print(f"File {file_name} loaded to {output_folder}.")

# when the file doesnot match with config table it moves bigquery as a table
def load_to_bigquery(bq_client, bucket_name, output_folder, file_name, table_name):
    file_url = f"gs://{bucket_name}/{output_folder}/{file_name}".replace('.prn', '.csv')
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq_client.load_table_from_uri(
        file_url, f"{DATASET_ID}.{table_name}", job_config=job_config
    )
    job.result()
    print(f"File {file_name} loaded to BigQuery table as {table_name}.")

#Extract configuration data from BigQuery.
def extract_config_data(bq_client):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{CONFIG_TABLE}`"
    query_job = bq_client.query(query)
    return [dict(row) for row in query_job.result()]

# Main function to validate and process files
def validate_and_process_files():
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    config_data = extract_config_data(bq_client)

    blobs = bucket.list_blobs(prefix=f"{INPUT_FOLDER_NAME}/", delimiter='/')
    file_paths = [blob.name for blob in blobs if '.' in blob.name.split('/')[-1] and blob.name.endswith('.prn')]

    for file_path in file_paths:
        blob = bucket.blob(file_path)
        file_content = blob.download_as_string().decode("utf-8", errors="replace")
        file_name = file_path.split('/')[-1]

        for config_dict in config_data:
            if config_dict['file_name'] == file_name:
                column_info = parse_json_with_starting_position(config_dict['column_info'])
                processed_lines = process_fixed_length_file(file_content, column_info)

                
                if processed_lines and config_dict['archive_flag'] == 'Y':
                    csv_content = fixed_length_file_to_csv(processed_lines, column_info)
                    upload_to_cloud_storage(BUCKET_NAME, OUTPUT_FOLDER_NAME, file_name, csv_content)    
                elif config_dict['archive_flag'] == 'N':
                    load_to_bigquery(bq_client, BUCKET_NAME, OUTPUT_FOLDER_NAME, file_name, config_dict['bq_table_name'])
                else:
                    print(f"Invalid archive_flag value for file {file_name}: {config_dict['archive_flag']}")
       

if __name__ == "__main__":
    validate_and_process_files()
