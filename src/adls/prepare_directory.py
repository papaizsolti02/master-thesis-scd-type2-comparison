import os
import time

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient


def _new_filesystem_client(connection_string: str, filesystem_name: str):
    service = DataLakeServiceClient.from_connection_string(connection_string)
    return service.get_file_system_client(filesystem_name)


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

    print("Waiting 10 seconds for previous filesystem deletion to complete...", flush=True)
    time.sleep(10)

    max_retries = 5
    filesystem = _new_filesystem_client(connection_string, filesystem_name)

    for attempt in range(max_retries):
        try:
            filesystem.create_file_system()
            print(f"Created ADLS filesystem: {filesystem_name}", flush=True)
        except ResourceExistsError:
            print(f"ADLS filesystem already exists: {filesystem_name}", flush=True)
        except ResourceNotFoundError as e:
            if attempt < max_retries - 1:
                wait_time = 3 * (attempt + 1)
                print(f"Filesystem creation attempt {attempt + 1} failed: {e}", flush=True)
                print(f"Waiting {wait_time} seconds before retry...", flush=True)
                time.sleep(wait_time)
                filesystem = _new_filesystem_client(connection_string, filesystem_name)
                continue
            print(f"Failed to create filesystem after {max_retries} attempts", flush=True)
            raise

        for readiness_attempt in range(max_retries):
            try:
                filesystem.get_file_system_properties()
                print(f"ADLS filesystem is ready: {filesystem_name}", flush=True)
                break
            except ResourceNotFoundError as e:
                if readiness_attempt < max_retries - 1:
                    wait_time = 3 * (readiness_attempt + 1)
                    print(f"Filesystem not ready yet: {e}", flush=True)
                    print(f"Waiting {wait_time} seconds before rechecking...", flush=True)
                    time.sleep(wait_time)
                    filesystem = _new_filesystem_client(connection_string, filesystem_name)
                else:
                    print(f"Filesystem did not become ready after {max_retries} attempts", flush=True)
                    raise
        else:
            continue

        break

    directory = filesystem.get_directory_client(directory_name)

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
                wait_time = 4 * (attempt + 1)
                print(f"Directory creation attempt {attempt + 1} failed: {e}", flush=True)
                print(f"Waiting {wait_time} seconds before retry...", flush=True)
                time.sleep(wait_time)
                filesystem = _new_filesystem_client(connection_string, filesystem_name)
                directory = filesystem.get_directory_client(directory_name)
            else:
                print(f"Failed to create directory after {max_retries} attempts", flush=True)
                raise


if __name__ == "__main__":
    main()
