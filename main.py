import csv
import json
import os

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

def update_csv(csv_path):
    updated_rows = []

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        total_rows = sum(1 for _ in csv_reader)  # Count the total number of rows

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Use tqdm to display progress while updating CSV
        for row in tqdm(csv_reader, total=total_rows, desc="Updating CSV", unit="rows"):
            category_option_combo = row.get('categoryoptioncombo', '')
            attribute_option_combo = row.get('attributeoptioncombo', '')

            if category_option_combo == 'c6PwdArn3fZ' or attribute_option_combo == 'c6PwdArn3fZ':
                row['categoryoptioncombo'] = 'HllvX50cXC0'
                row['attributeoptioncombo'] = 'HllvX50cXC0'
            elif category_option_combo == 'another_value' or attribute_option_combo == 'another_value':
                row['categoryoptioncombo'] = 'replacement_value'
                row['attributeoptioncombo'] = 'replacement_value'

            updated_rows.append(row)

    field_names = updated_rows[0].keys() if updated_rows else []

    temp_csv_path = "temp_updated.csv"

    with open(temp_csv_path, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()
        csv_writer.writerows(updated_rows)

    os.remove(csv_path)  # Delete the old CSV file
    os.rename(temp_csv_path, csv_path)  # Rename the updated CSV file


def convert_csv_to_json(csv_path, json_path, data_set_id):
    json_data = {
        "dataSet": data_set_id,
        "dataValues": []
    }

    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        # Use tqdm to display progress while reading CSV
        for row in tqdm(csv_reader, desc="Converting CSV to JSON", unit="rows"):
            entry = row
            if entry.get('categoryoptioncombo') == 'c6PwdArn3fZ' or entry.get(
                    'attributeoptioncombo') == 'c6PwdArn3fZ':
                entry['categoryoptioncombo'] = 'HllvX50cXC0'
                entry['attributeoptioncombo'] = 'HllvX50cXC0'
            elif entry.get('categoryoptioncombo') == 'c6PwdArn3fZ' or entry.get(
                    'attributeoptioncombo') == 'c6PwdArn3fZ':
                entry['categoryoptioncombo'] = 'HllvX50cXC0'
                entry['attributeoptioncombo'] = 'HllvX50cXC0'

            json_data["dataValues"].append(entry)

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
    for file in os.listdir(downloads_directory):
        if file == ".gitignore":
            continue  # Skip files with the name ".keep"

        file_path = os.path.join(downloads_directory, file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
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
        save_path = f"downloads/{data_set}_dataSet.csv"
        download_csv_by_dataSet(username, password, data_set, save_path)
