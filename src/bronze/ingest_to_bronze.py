import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from src.utils.gcs_helpers import upload_file
from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
RAW_BATCH_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
STREAMING_DIR = Path(__file__).resolve().parents[2] / "data" / "bronze_local" / "streaming"


def upload_batch(data_ingestao: str) -> None:
    for local_path in RAW_BATCH_DIR.glob("*.parquet"):
        table_id = local_path.stem
        blob_path = f"bronze/batch/{table_id}/data_ingestao={data_ingestao}/{local_path.name}"
        upload_file(BUCKET_NAME, local_path, blob_path)
        logger.info("Batch: %s -> gs://%s/%s", local_path.name, BUCKET_NAME, blob_path)


def upload_streaming() -> None:
    if not STREAMING_DIR.exists():
        logger.info("Nenhum dado de streaming local encontrado em %s", STREAMING_DIR)
        return

    for partition_dir in STREAMING_DIR.glob("data_ingestao=*"):
        for local_path in partition_dir.glob("*.parquet"):
            blob_path = f"bronze/streaming/{partition_dir.name}/{local_path.name}"
            upload_file(BUCKET_NAME, local_path, blob_path)
            logger.info("Streaming: %s -> gs://%s/%s", local_path.name, BUCKET_NAME, blob_path)


def main() -> None:
    data_ingestao = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    upload_batch(data_ingestao)
    upload_streaming()


if __name__ == "__main__":
    main()
