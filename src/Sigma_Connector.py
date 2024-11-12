import os
import asyncio
import requests
import logging
import shutil
from pathlib import Path
from configparser import ConfigParser
import mysql.connector as mysqli
from datetime import datetime, timedelta
import base64  # Import base64 module


# Load configuration
config = ConfigParser()
config.read('config.ini')
hospital_name = config.get('DEFAULT', 'hospitalName')
tmp_folder_count = config.getint('DEFAULT', 'tmpFolderCount')
sql_file_size_limit = config.getint('DATABASE', 'sqlFileSizeLimit')
# New entry to get the base64 encoded link
get = config.get('DEFAULT', 'get')
post = config.get('DEFAULT', 'post')

def decode_get(encoded_data):
    """Decode double encoded internet checking get string.   https://radiolims.sigmentech.com  """
    try:
        decoded_data = base64.b64decode(encoded_data)
        decoded_data = base64.b64decode(decoded_data)
        return decoded_data
    except Exception as e:
        logging.error(f"Error decoding get data: {e}")
        return None
def decode_post(encoded_data):
    """Decode double encoded api post string.    https://sigma.pacs.sigmentech.com/studies   """
    try:
        decoded_data = base64.b64decode(encoded_data)
        decoded_data = base64.b64decode(decoded_data)
        return decoded_data
    except Exception as e:
        logging.error(f"Error decoding post data: {e}")
        return None
    
# Encode Sample function
# def encode_data(data):
#     """Encode data using base64."""
#     try:
#         encoded_data = base64.b64encode(data.encode('utf-8'))
#         encoded_data = base64.b64encode(encoded_data)
#         print(encoded_data)
#         return encoded_data
#     except Exception as e:
#         logging.error(f"Error encoding data: {e}")
#         return None

# Decode the base64 link
get_link = decode_get(get).decode('utf-8')
post_link = decode_post(post).decode('utf-8')

# New database connection details from config
try:
    table_name = config.get('DATABASE', 'table_name')

except Exception as e:
    logging.error(f"Error loading database configuration: {e}")
    raise  # Re-raise the exception to stop the application if critical

log_file_path = Path(__file__).parent / 'logs.txt'
error_data_path = Path(__file__).parent / 'error_data'
temp_data_path = Path(__file__).parent / 'temp_data'
sql_backup_path = Path(__file__).parent / 'sql_backup'

logging.basicConfig(filename=log_file_path, level=logging.INFO)

data_folder_path = Path(__file__).parent / 'data'

async def main():

    # Start the move_back_from_error_data task
    # Run this in the background
    asyncio.create_task(move_back_from_error_data())
    # Run the temp folder limit check in the background
    asyncio.create_task(check_temp_folder_limit())

    while True:
        try:
            # print("Checking for new files in data folder...")
            folders = [f for f in data_folder_path.iterdir() if f.is_dir()]
            # print(f"Folders found: {folders}")

            if not folders:
                # print("No new folders found, sleeping for 60 seconds...")
                await asyncio.sleep(60)
                continue

            for folder in folders:
                # Check for subfolders within the current folder
                subfolders = [f for f in folder.iterdir() if f.is_dir()]
                for subfolder in subfolders:
                    if any(subfolder.iterdir()):  # Check if the subfolder is not empty
                        # Move the subfolder to the data directory
                        new_location = data_folder_path / subfolder.name
                        shutil.move(str(subfolder), str(new_location))
                        logging.info(f"Moved folder {subfolder} to {
                                     data_folder_path}")
                        print(f"Moved folder {subfolder} to {
                              data_folder_path}")

                # Remove the original folder if it's empty after moving subfolders
                if not any(folder.iterdir()):  # Check if the folder is now empty
                    shutil.rmtree(folder)
                    logging.info(f"Removed empty folder: {folder}")
                    print(f"Removed empty folder: {folder}")

                # print(f"Processing folder: {folder}")
                files = sorted(get_dcm_file_paths(folder),
                               key=lambda f: os.path.getctime(f))
                print(f"Files found (sorted by creation time): {files}")

                if not files:
                    # print("No files found in folder, skipping...")
                    continue

                # Open files and store file handles
                file_handles = []
                folder_moved = False  # Flag to track if the folder has been moved
                try:
                    # Include folder name
                    post_data = {'file': [(open(file, 'rb'), file)
                                          for file in files], 'folder_name': folder.name}
                    # Store file handles
                    file_handles = [fh[0] for fh in post_data['file']]
                    # print("Checking internet connection...")
                    has_internet_access = await is_connected()
                    print(f"Internet access: {has_internet_access}")

                    if has_internet_access:
                        # print("Uploading studies...")
                        successful_files, failed_files = await upload_studies(post_data)
                        print(f"Successful uploads: {successful_files}")
                        print(f"Failed uploads: {failed_files}")

                        # Only create a unique folder in temp_data if there are successful uploads
                        if successful_files:
                            new_folder_path = temp_data_path / folder.name
                            # Ensure the folder exists
                            new_folder_path.mkdir(parents=True, exist_ok=True)

                            # Move successful files to temp_data
                            for file_name in successful_files:
                                original_file_path = Path(folder) / file_name
                                move_dcm_folder(
                                    original_file_path, new_folder_path)

                        # Only move the folder to error_data if there are failed uploads
                        if failed_files:
                            # Move failed files to error_data
                            for file_name in failed_files:
                                original_file_path = Path(folder) / file_name
                                move_to_error_data(original_file_path)
                            # Move the entire folder to error_data if there were any failed uploads
                            move_to_error_data(folder)
                            folder_moved = True  # Set the flag to true

                    else:
                        logging.error("No Internet Access!!")
                        print("No internet access, sleeping for 5 seconds...")
                        await asyncio.sleep(5)
                except Exception as e:
                    logging.error(f"Error during processing: {e}")
                    if not folder_moved:  # Move to error_data only if not already moved
                        move_to_error_data(folder)
                        folder_moved = True  # Set the flag to true
                finally:
                    # Ensure all file handles are closed
                    for fh in file_handles:
                        fh.close()

                    # Check if the folder still exists before moving
                    if folder.exists() and not folder_moved:
                        # If the folder was not moved due to errors, we can delete it
                        shutil.rmtree(folder)  # Delete the original folder
                        logging.info(f"Original folder deleted: {folder}")
                        # print(f"Original folder deleted: {folder}")
        except Exception as e:
            logging.error(f"Error in Try file {e}")


async def upload_studies(post_data):
    successful_files = []
    failed_files = []
    try:
        # print("Sending POST request to upload studies...")

        # Correctly format the files parameter
        files = {Path(file_path).name: file_handle for file_handle,
                 file_path in post_data['file']}

        # Log the files being sent
        # print(f"Files being uploaded: {list(files.keys())}")

        # Create a list to hold the upload tasks
        upload_tasks = []

        for file_name, file_handle in files.items():
            upload_tasks.append(upload_file(
                # Create upload tasks
                file_name, file_handle, post_data['folder_name'], successful_files, failed_files))

        # Limit concurrent uploads to 4
        for i in range(0, len(upload_tasks), 4):
            # Upload 4 files at a time
            await asyncio.gather(*upload_tasks[i:i + 4])

    finally:
        # Ensure all file handles are closed
        for file_handle in post_data['file']:
            try:
                file_handle[0].close()  # Close the file handle correctly
            except Exception as e:
                logging.error(f"Error closing file {file_handle[1]}: {e}")

    return successful_files, failed_files


async def upload_file(file_name, file_handle, folder_name, successful_files, failed_files):
    try:
        response = requests.post(
            post_link,
            files={file_name: file_handle},
            headers={'next_api': 'CALL_PHP_API',
                     'hospital_name': hospital_name}
        )
        response.raise_for_status()
        successful_files.append(file_name)  # Track successful uploads
        logging.info(f"Upload Success for {file_name}")
        print(f"Upload successful for: {file_name}")

        # Access the patient name from the folder name
        patient_name = folder_name  # Use folder name as patient name
        if patient_name:
            # Prepare SQL for successful upload
            upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql_file_path = sql_backup_path / 'record.sql'
            
            # Check if the record.sql file size exceeds the limit from config
            if sql_file_path.exists() and sql_file_path.stat().st_size >= sql_file_size_limit * 1024 * 1024:
                # Rename the existing record.sql file with the current date
                new_sql_file_name = f"record_{datetime.now().strftime('%Y%m%d')}.sql"
                new_sql_file_path = sql_backup_path / new_sql_file_name
                sql_file_path.rename(new_sql_file_path)  # Rename the file
                logging.info(f"Renamed SQL file to: {new_sql_file_path}")
                print(f"Renamed SQL file to: {new_sql_file_path}")

            # Write to the new record.sql file
            with open(sql_file_path, 'a') as sql_file:
                sql_file.write(f"INSERT INTO {table_name} (patient_name, hospital_name, upload_time, status_flag, error_time, image_name) VALUES ('{patient_name}', '{hospital_name}', '{upload_time}', 'T', NULL, '{file_name}');\n")
            print(f"Upload recorded in SQL file for patient: {patient_name}")

    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred for {file_name}: {err}")
        print(f"HTTP error occurred for {file_name}: {err}")
        failed_files.append(file_name)  # Track failed uploads
        # Prepare SQL for failed upload
        patient_name = folder_name  # Use folder name as patient name
        if patient_name:
            upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql_file_path = sql_backup_path / 'record.sql'
            
            # Check if the record.sql file size exceeds the limit from config
            if sql_file_path.exists() and sql_file_path.stat().st_size >= sql_file_size_limit * 1024 * 1024:
                # Rename the existing record.sql file with the current date
                new_sql_file_name = f"record_{datetime.now().strftime('%Y%m%d')}.sql"
                new_sql_file_path = sql_backup_path / new_sql_file_name
                sql_file_path.rename(new_sql_file_path)  # Rename the file
                logging.info(f"Renamed SQL file to: {new_sql_file_path}")
                print(f"Renamed SQL file to: {new_sql_file_path}")

            # Write to the new record.sql file
            with open(sql_file_path, 'a') as sql_file:
                sql_file.write(f"INSERT INTO {table_name} (patient_name, hospital_name, upload_time, status_flag, error_time, image_name) VALUES ('{patient_name}', '{hospital_name}', NULL, 'H', '{error_time}', '{file_name}');\n")
            print(f"Failed upload recorded in SQL file for patient: {patient_name}")

    except Exception as err:
        logging.error(f"Other error occurred for {file_name}: {err}")
        print(f"Other error occurred for {file_name}: {err}")
        failed_files.append(file_name)  # Track failed uploads
        # Prepare SQL for failed upload
        patient_name = folder_name  # Use folder name as patient name
        if patient_name:
            upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql_file_path = sql_backup_path / 'record.sql'
            
            # Check if the record.sql file size exceeds the limit from config
            if sql_file_path.exists() and sql_file_path.stat().st_size >= sql_file_size_limit * 1024 * 1024:
                # Rename the existing record.sql file with the current date
                new_sql_file_name = f"record_{datetime.now().strftime('%Y%m%d')}.sql"
                new_sql_file_path = sql_backup_path / new_sql_file_name
                sql_file_path.rename(new_sql_file_path)  # Rename the file
                logging.info(f"Renamed SQL file to: {new_sql_file_path}")
                print(f"Renamed SQL file to: {new_sql_file_path}")

            # Write to the new record.sql file
            with open(sql_file_path, 'a') as sql_file:
                sql_file.write(f"INSERT INTO {table_name} (patient_name, hospital_name, upload_time, status_flag, error_time, image_name) VALUES ('{patient_name}', '{hospital_name}', NULL, 'F', '{error_time}', '{file_name}');\n")
            print(f"Failed upload recorded in SQL file for patient: {patient_name}")


async def move_back_from_error_data():
    while True:  # Keep this running indefinitely
        print("Checking if error_data_path exists...")
        if error_data_path.exists():
            # print(f"Iterating over folders in: {error_data_path}")
            for error_folder in error_data_path.iterdir():
                if error_folder.is_dir():
                    print(f"Found directory: {error_folder}")

                    # Move the folder back to the data directory
                    data_folder_path = str(error_folder).replace(
                        'error_data', 'data')
                    print(f"Preparing to move folder to: {data_folder_path}")

                    # Check if the destination folder exists
                    if Path(data_folder_path).exists():
                        # If it exists, merge files from error_folder to data_folder_path
                        for error_file in error_folder.iterdir():
                            if error_file.is_file():
                                destination_file = Path(
                                    data_folder_path) / error_file.name
                                if not destination_file.exists():  # Only move if the file doesn't exist
                                    print(f"Moving file: {error_file} to {
                                          data_folder_path}")
                                    shutil.move(str(error_file),
                                                str(destination_file))
                                    logging.info(f"Moved file back to data: {
                                                 destination_file}")
                                    print(f"Moved file back to data: {
                                          destination_file}")
                                else:
                                    print(f"File already exists in destination: {
                                          destination_file}, skipping move.")

                        # After merging, check if the error folder is empty before removing it
                        if not any(error_folder.iterdir()):  # Check if the folder is empty
                            shutil.rmtree(str(error_folder))
                            logging.info(f"Removed empty error folder: {
                                         error_folder}")
                            print(f"Removed empty error folder: {
                                  error_folder}")
                    else:
                        print(f"Moving folder: {error_folder} to {
                              data_folder_path}")
                        shutil.move(str(error_folder), data_folder_path)
                        logging.info(f"Moved folder back to data: {
                                     data_folder_path}")
                        print(f"Moved folder back to data: {data_folder_path}")
        else:
            print("Error data path does not exist.")

        await asyncio.sleep(120)  # Sleep for 120 seconds before checking again


async def check_temp_folder_limit():
    while True:
        try:
            # Check the number of folders in temp_data
            temp_folders = sorted(temp_data_path.iterdir(),
                                  key=lambda f: f.stat().st_ctime)
            if len(temp_folders) >= tmp_folder_count:  # Check if the limit is reached
                # Calculate how many folders to remove
                # +1 to remove extra the oldest
                folders_to_remove = len(temp_folders) - tmp_folder_count + 0
                for folder in temp_folders[:folders_to_remove]:
                    shutil.rmtree(folder)  # Remove the oldest folder
                    logging.info(f"Removed old folder: {folder}")
                    print(f"Removed old folder: {folder}")

        except Exception as e:
            logging.error(f"Error checking temp folder limit: {e}")
            print(f"Error checking temp folder limit: {e}")

        await asyncio.sleep(600)  # Sleep for 10 minutes (600 seconds)


def get_dcm_file_paths(folder):
    # print(f"Getting DCM file paths in folder: {folder}")
    all_files = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            all_files.append(os.path.join(root, file))
    # print(f"DCM files found: {all_files}")
    return all_files


def move_dcm_folder(old_path, new_path):
    # print(f"Moving DCM folder from {old_path} to {new_path}")
    if not temp_data_path.exists():
        temp_data_path.mkdir(parents=True)

    try:
        # Check if the old path is a directory
        if old_path.is_dir():
            # Move the entire directory structure to the new path
            for root, dirs, files in os.walk(old_path):
                # Create corresponding subdirectories in new path
                rel_path = os.path.relpath(root, old_path)
                new_dir = new_path / rel_path
                new_dir.mkdir(parents=True, exist_ok=True)

                # Move each file while preserving directory structure
                for file in files:
                    old_file = Path(root) / file
                    new_file = new_dir / file
                    shutil.move(str(old_file), str(new_file))
                    logging.info(f"File moved, old path: {
                                 old_file}, new path: {new_file}")
                    print(f"File moved to temp_data: {new_file}")

            # Remove original directory after moving all contents
            shutil.rmtree(old_path)
            logging.info(f"Original directory removed: {old_path}")
        else:
            # Move single file to the new path
            shutil.move(str(old_path), str(new_path / old_path.name))
            logging.info(f"File moved, old path: {old_path}, new path: {
                         new_path / old_path.name}")
            print(f"File moved to temp_data: {new_path / old_path.name}")
    except Exception as e:
        logging.error(f"Error moving file from {old_path} to {new_path}: {e}")
        print(f"Error moving file: {e}")


def move_to_error_data(folder):
    print(f"Moving folder to error_data: {folder}")
    if not error_data_path.exists():
        error_data_path.mkdir(parents=True)

    error_folder_path = str(folder).replace('data', 'error_data')

    # Check if the destination path already exists
    if Path(error_folder_path).exists():
        # Remove the existing folder to replace it
        shutil.rmtree(error_folder_path)
        logging.info(f"Existing error folder deleted: {error_folder_path}")
        print(f"Existing error folder deleted: {error_folder_path}")

    # Move the new folder to error_data
    shutil.move(folder, error_folder_path)
    logging.info(f"Folder moved to error_data, old path: {
                 folder}, new path: {error_folder_path}")
    print(f"Folder moved to error_data: {error_folder_path}")

    # Ensure the original folder is deleted only if it still exists
    if folder.exists():
        shutil.rmtree(folder)  # Delete the original folder
        logging.info(f"Original folder deleted: {folder}")
        print(f"Original folder deleted: {folder}")


async def is_connected():
    try:
        print("Checking connection to the decoded link...")
        # Use get_link in the request
        requests.get(get_link, timeout=5)
        print("Internet connection is available.")
        return True
    except requests.ConnectionError:
        print("No internet connection.")
        return False


if __name__ == "__main__":
    asyncio.run(main())
