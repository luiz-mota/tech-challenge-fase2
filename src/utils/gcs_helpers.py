import os
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account


def get_credentials(scopes: list[str]) -> service_account.Credentials:
    return service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=scopes,
    )


def get_storage_client() -> storage.Client:
    credentials = get_credentials(["https://www.googleapis.com/auth/devstorage.read_write"])
    return storage.Client(credentials=credentials, project=os.environ["GCP_PROJECT_ID"])


def upload_file(bucket_name: str, local_path: Path, blob_path: str) -> None:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))


def download_file(bucket_name: str, blob_path: str, local_path: Path) -> None:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(local_path))


def get_latest_partition(bucket_name: str, prefix: str) -> str:
    """Retorna o nome da partição data_ingestao=YYYY-MM-DD mais recente sob um prefixo."""
    client = get_storage_client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    partitions = set()
    for blob in blobs:
        for part in blob.name.split("/"):
            if part.startswith("data_ingestao="):
                partitions.add(part)
    if not partitions:
        raise FileNotFoundError(f"Nenhuma partição encontrada em gs://{bucket_name}/{prefix}")
    return max(partitions)
