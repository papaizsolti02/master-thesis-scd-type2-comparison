import os


ADLS_FILE_SYSTEM = os.getenv("DATALAKE_FILE_SYSTEM", "generated-data")
ADLS_DIRECTORY = os.getenv("DATALAKE_DIRECTORY", "daily")

def upload_snapshots(snapshot_dir):
    try:
        from azure.core.exceptions import ResourceExistsError
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        raise ImportError(
            "Missing Azure SDK. Install dependencies with: pip install -r requirements.txt"
        ) from exc

    connection_string = os.getenv("DATALAKE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("DATALAKE_CONNECTION_STRING is required.")

    service = DataLakeServiceClient.from_connection_string(connection_string)
    file_system_client = service.get_file_system_client(file_system=ADLS_FILE_SYSTEM)

    try:
        file_system_client.create_file_system()
    except ResourceExistsError:
        pass

    directory_client = file_system_client.get_directory_client(ADLS_DIRECTORY)
    try:
        directory_client.create_directory()
    except ResourceExistsError:
        pass

    uploaded = 0
    for local_file in sorted(snapshot_dir.glob("*.csv")):
        file_client = directory_client.get_file_client(local_file.name)
        with local_file.open("rb") as file_handle:
            file_client.upload_data(file_handle.read(), overwrite=True)
        uploaded += 1
        print(f"Uploaded {local_file.name} to ADLS ({uploaded} files uploaded so far)", flush=True)

    return uploaded
