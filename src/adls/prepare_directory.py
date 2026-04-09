import os
import time

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient


def main() -> None:
    connection_string = os.environ.get("DATALAKE_CONNECTION_STRING")
    filesystem_name = os.environ.get("DATALAKE_FILE_SYSTEM")
    directory_name = os.environ.get("DATALAKE_DIRECTORY")

    if not connection_string:
        raise ValueError("DATALAKE_CONNECTION_STRING is required.")
    if not filesystem_name:
        raise ValueError("DATALAKE_FILE_SYSTEM is required.")
    if not directory_name:
        raise ValueError("DATALAKE_DIRECTORY is required.")

    service = DataLakeServiceClient.from_connection_string(connection_string)
    filesystem = service.get_file_system_client(filesystem_name)

    # Create filesystem with retry for timing issues
    max_retries = 3
    for attempt in range(max_retries):
        try:
            filesystem.create_file_system()
            print(f"Created ADLS filesystem: {filesystem_name}")
            break
        except ResourceExistsError:
            print(f"ADLS filesystem already exists: {filesystem_name}")
            break
        except ResourceNotFoundError:
            if attempt < max_retries - 1:
                print(f"Filesystem creation attempt {attempt + 1} failed, retrying in 1 second...")
                time.sleep(1)
            else:
                raise

    # Additional delay to ensure filesystem is fully ready
    time.sleep(2)

    directory = filesystem.get_directory_client(directory_name)
    # Retry directory creation with exponential backoff
    for attempt in range(max_retries):
        try:
            directory.create_directory()
            print(f"Created ADLS directory: {filesystem_name}/{directory_name}")
            break
        except ResourceExistsError:
            print(f"ADLS directory already exists: {filesystem_name}/{directory_name}")
            break
        except ResourceNotFoundError as e:
            if attempt < max_retries - 1:
                print(f"Directory creation attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {1 + attempt} seconds...")
                time.sleep(1 + attempt)
            else:
                print(f"Failed to create directory after {max_retries} attempts")
                raise


if __name__ == "__main__":
    main()
