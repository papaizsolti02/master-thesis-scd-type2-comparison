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

    print(f"Waiting 5 seconds for previous filesystem deletion to complete...", flush=True)
    time.sleep(5)

    service = DataLakeServiceClient.from_connection_string(connection_string)
    filesystem = service.get_file_system_client(filesystem_name)

    # Create filesystem with retry for timing issues
    max_retries = 5
    filesystem_created = False
    for attempt in range(max_retries):
        try:
            filesystem.create_file_system()
            print(f"Created ADLS filesystem: {filesystem_name}", flush=True)
            filesystem_created = True
            break
        except ResourceExistsError:
            print(f"ADLS filesystem already exists: {filesystem_name}", flush=True)
            filesystem_created = True
            break
        except ResourceNotFoundError as e:
            if attempt < max_retries - 1:
                print(f"Filesystem creation attempt {attempt + 1} failed: {e}", flush=True)
                print(f"Waiting 3 seconds before retry...", flush=True)
                time.sleep(3)
            else:
                print(f"Failed to create filesystem after {max_retries} attempts", flush=True)
                raise

    if not filesystem_created:
        raise RuntimeError("Filesystem was not created or verified to exist")

    # Create a fresh service client after filesystem is ready
    print(f"Creating fresh client connection to filesystem...", flush=True)
    service = DataLakeServiceClient.from_connection_string(connection_string)
    filesystem = service.get_file_system_client(filesystem_name)
    directory = filesystem.get_directory_client(directory_name)

    # Retry directory creation with extended backoff
    for attempt in range(max_retries):
        try:
            directory.create_directory()
            print(f"Created ADLS directory: {filesystem_name}/{directory_name}", flush=True)
            break
        except ResourceExistsError:
            print(f"ADLS directory already exists: {filesystem_name}/{directory_name}", flush=True)
            break
        except ResourceNotFoundError as e:
            if attempt < max_retries - 1:
                print(f"Directory creation attempt {attempt + 1} failed: {e}", flush=True)
                wait_time = 2 * (attempt + 1)
                print(f"Waiting {wait_time} seconds before retry...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"Failed to create directory after {max_retries} attempts", flush=True)
                raise


if __name__ == "__main__":
    main()
