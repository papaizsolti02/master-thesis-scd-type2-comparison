import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient


def main() -> None:
    connection_string = os.environ.get("DATALAKE_CONNECTION_STRING")
    filesystem_name = os.environ.get("DATALAKE_FILE_SYSTEM")

    if not connection_string:
        raise ValueError("DATALAKE_CONNECTION_STRING is required.")
    if not filesystem_name:
        raise ValueError("DATALAKE_FILE_SYSTEM is required.")

    service = DataLakeServiceClient.from_connection_string(connection_string)
    filesystem = service.get_file_system_client(filesystem_name)

    try:
        filesystem.delete_file_system()
        print(f"Deleted ADLS filesystem: {filesystem_name}", flush=True)
    except ResourceNotFoundError:
        print(f"ADLS filesystem did not exist: {filesystem_name}", flush=True)


if __name__ == "__main__":
    main()
