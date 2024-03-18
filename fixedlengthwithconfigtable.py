from google.cloud import storage, bigquery
import json
import os

# Service account credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/User/Downloads/dbt-bq-414505-6e87b3af4e2d.json"

# Function to parse JSON data string and add starting position
def parse_json_with_starting_position(json_data_string):
    # Parse the JSON data string into a Python dictionary
    config_data_dict = json.loads(json_data_string)
    
    # Initialize starting position
    starting_position = 1
    
    # Add starting position to each field dictionary
    for field_name, field_info in config_data_dict.items():
        field_info['starting_position'] = starting_position
        starting_position += field_info['number_of_positions']
    
    return config_data_dict

# Function to validate and process fixed-length files
def fixed_length_file_validation(input_folder_name, file_path, bucket_name, config_data, output_folder_name):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Check if the file has a .prn extension
    if not file_path.endswith('.prn'):
        print(f"Skipping file {file_path}: Not a .prn file")
        return
    
    # Download fixed-length file content
    file_content = blob.download_as_string().decode("utf-8", errors="replace")
    # file_lines = file_content.split("\n")
    # for line in file_lines:
    #     print(line)

    # Extract file name from file path
    file_name = file_path.split('/')[-1]

    # Extract configuration data for validation
    for config_dict in config_data:
        if config_dict['file_name'] == file_name:
            column_info = parse_json_with_starting_position(config_dict['column_info'])
            expected_columns = len(column_info)
            record_length = max(field['starting_position'] + field['number_of_positions'] - 1 for field in column_info.values())

            # Process each line in the fixed-length file
            processed_lines = []
            for line in file_content.splitlines():
                if len(line) == record_length:
                    record = {}
                    for field_name, field_info in column_info.items():
                        starting_pos = field_info['starting_position'] - 1
                        length = field_info['number_of_positions']
                        value = line[starting_pos:starting_pos+length].strip()
                        record[field_name] = value
                    processed_lines.append(record)
            # print(processed_lines)

            if len(processed_lines) > 0 and len(processed_lines[0]) == expected_columns:
                if config_dict['archive_flag'] == 'Y':
                    # Copy the validated file to the output folder
                    destination_blob = bucket.blob(f"{output_folder_name}/{file_name}".replace('.prn', '.csv'))
                    destination_blob.upload_from_string(file_content)
                    print(f"File loaded to {output_folder_name}")
                else:
                    # Load the fixed-length file into BigQuery
                    file_url = f"gs://{bucket_name}/{file_path}"
                    job_config = bigquery.LoadJobConfig(
                        autodetect=True,
                        source_format=bigquery.SourceFormat.CSV,
                        # schema=[bigquery.SchemaField(field_name, "STRING", mode="NULLABLE") for field_name in column_info.keys()]
                    )
                    table_name = config_dict['bq_table_name']
                    job = bq_client.load_table_from_uri(
                        file_url, f"{dataset_id}.{table_name}", job_config=job_config
                    )
                    job.result()
                    print("Loaded in BigQuery")
            else:
                print(f"Number of columns in {file_name} does not match the expected columns")
        else:
            print(f"{file_name} not matched with {config_table}")

# Function to extract configuration data from BigQuery
def extract_config_data():
    # Initialize BigQuery client
    bq_client = bigquery.Client()
    
    # Query the configuration table in BigQuery
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{config_table}`"
    query_job = bq_client.query(query)
    results = query_job.result()

    # Convert query results to a list of dictionaries
    config_data = []
    for row in results:
        row_dict = {}
        for field in results.schema:
            row_dict[field.name] = row[field.name]
        config_data.append(row_dict)

    return config_data

# Main function
def main():
    # Define input and output details
    bucket_name = "migration_project"
    input_folder_name = 'input_file'
    output_folder_name = 'output_file'

    # Get the list of fixed-length files in the input folder
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_folder_name + '/', delimiter='/')
    file_paths = [blob.name for blob in blobs if '.' in blob.name.split('/')[-1]]

    # Extract configuration data from BigQuery
    config_data = extract_config_data()

    # Validate and process each fixed-length file
    for file_path in file_paths:
        if file_path.endswith('.prn'):  # Assuming fixed-length files have .prn extension
            fixed_length_file_validation(input_folder_name, file_path, bucket_name, config_data, output_folder_name)
        else:
            print(f"Skipping file {file_path}: Unsupported file format")

# Project and dataset details
project_id = "dbt-bq-414505"
dataset_id = "dbt_bq_dataset_123"
config_table = "config_fixedlength3"

# Initialize BigQuery client
bq_client = bigquery.Client()

# Execute the main function
if __name__ == "__main__":
    main()
