import csv
import json
import os
import shutil

import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm  # Import tqdm for progress bar


def download_csv_with_authentication(url, username, password, save_path):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), stream=True,
                                timeout=300)  # Adjust timeout as needed
        response.raise_for_status()  # Raise an exception for HTTP errors

        total_size = int(response.headers.get('content-length', 0))
        chunk_size = 1024  # Adjust chunk size as desired

        with open(save_path, 'wb') as csv_file, tqdm(
                desc=save_path,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                csv_file.write(chunk)
                bar.update(len(chunk))

        print(f"CSV file downloaded and saved to {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return False


# def post_json_data(json_data):
#     print("Posting Json data")
#     try:
#         # URL for posting JSON data
#         url = "https://ug.sk-engine.cloud/int2/api/dataValueSets"
#         # Your authentication credentials
#         post_username = "admin"
#         post_password = "district"
#         response = requests.post(url, auth=HTTPBasicAuth(post_username, post_password), json=json_data)
#         response.raise_for_status()  # Raise an exception for HTTP errors
#
#         print("JSON data posted successfully.")
#     except requests.exceptions.RequestException as e:
#         print(f"An error occurred: {e}")


def update_csv(csv_path, max_file_size_bytes=5 * 1024 * 1024):
    updated_rows = []
    current_file_size = 0
    current_file_index = 1
    dataset_name = os.path.splitext(os.path.basename(csv_path))[0]  # Extract dataset name

    # Create a folder for the dataset if it doesn't exist
    dataset_folder = os.path.join(os.path.dirname(csv_path), dataset_name).split("_")[0]
    os.makedirs(dataset_folder, exist_ok=True)

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        total_rows = sum(1 for _ in csv_reader)  # Count the total number of rows

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in tqdm(csv_reader, total=total_rows, desc="Updating CSV", unit="rows"):
            # Replace None values with an empty string
            row = {key: value if value is not None else "" for key, value in row.items()}

            category_option_combo = row.get('categoryoptioncombo', '')
            attribute_option_combo = row.get('attributeoptioncombo', '')

            if category_option_combo == 'c6PwdArn3fZ' or attribute_option_combo == 'c6PwdArn3fZ':
                row['categoryoptioncombo'] = 'HllvX50cXC0'
                row['attributeoptioncombo'] = 'HllvX50cXC0'

            updated_rows.append(row)
            current_file_size += len(",".join(row.values()))

            if current_file_size >= max_file_size_bytes:
                # If the current file size exceeds the specified limit, create a new file
                create_new_csv(updated_rows, dataset_folder, dataset_name, current_file_index)
                updated_rows = []
                current_file_size = 0
                current_file_index += 1

    # Create the last CSV file if there are remaining rows
    if updated_rows:
        create_new_csv(updated_rows, dataset_folder, dataset_name, current_file_index)

    # Clean up: Delete the old CSV file
    os.remove(csv_path)


def create_new_csv(rows, folder, dataset_name, index):
    # Define the subfolder where CSV parts will be saved
    csv_folder = os.path.join(folder, dataset_name, 'csv')
    os.makedirs(csv_folder, exist_ok=True)  # Ensure the subfolder exists

    field_names = rows[0].keys() if rows else []
    part_file_name = f"{dataset_name}_part_{index}.csv"
    part_file_path = os.path.join(csv_folder, part_file_name)

    with open(part_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()
        csv_writer.writerows(rows)


def convert_csv_to_json(csv_path, json_path, data_set_id):
    json_data = {
        "dataSet": data_set_id,
        "dataValues": []
    }

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Use tqdm to display progress while reading CSV
        for row in tqdm(csv_reader, desc="Converting CSV to JSON", unit="rows"):
            json_data["dataValues"].append(row)

    with open(json_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    # # Call the function to post JSON data
    # post_json_data(json_data)

    print(f"Converted CSV to JSON and saved to {json_path}")


def download_csv_by_dataSet(username, password, dataSet, save_path):
    base_url = "https://ima-assp.org/api/dataValueSets"
    params = {
        "dataElementIdScheme": "UID",
        "orgUnitIdScheme": "UID",
        "idScheme": "UID",
        "orgUnit": "s7ZjqzKnWsJ",
        "includeDeleted": "false",
        "children": "true",
        "startDate": "2013-01-01",
        "endDate": "2023-08-31",
        "dataSet": dataSet,
        "format": "csv",
        "attachment": "dataValueSets.csv"
    }
    url = f"{base_url}?{requests.compat.urlencode(params)}"

    success = download_csv_with_authentication(url, username, password, save_path)
    if success:
        # json_path = f"downloads/{dataSet}_dataSet.json"
        # convert_csv_to_json(save_path, json_path, dataSet)
        update_csv(save_path)
    else:
        with open("errors.log", "a") as error_log:
            error_log.write(f"Error downloading dataset ID: {dataSet}\n")


# Function to delete all CSV and JSON files in the directory
def delete_existing_files():
    downloads_directory = "downloads"
    for entry in os.listdir(downloads_directory):
        file_path = os.path.join(downloads_directory, entry)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                print(f"Deleted directory {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Your authentication credentials
    username = "Fifi_Manuel"
    password = "Hay@Figure8"

    # Read dataset IDs from dataSets.txt and download CSV files for each ID
    data_sets_file = "dataSets.txt"
    with open(data_sets_file, 'r') as f:
        data_sets = f.read().splitlines()

    # Delete previously generated CSV and JSON files
    delete_existing_files()

    for data_set in data_sets:
        save_path = f"downloads/IBAyM2I5Zfn_dataSet.csv"
        download_csv_by_dataSet(username, password, 'IBAyM2I5Zfn', save_path)
