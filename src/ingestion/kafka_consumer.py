import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "indicador_alfabetizacao"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "bronze_local" / "streaming"
IDLE_TIMEOUT_SECONDS = 10


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=IDLE_TIMEOUT_SECONDS * 1000,
        api_version=(2, 8, 2),
    )

    events = [message.value for message in consumer]
    consumer.close()

    if not events:
        logger.info("Nenhum evento novo consumido do tópico '%s'", TOPIC)
        return

    data_ingestao = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    partition_dir = OUTPUT_DIR / f"data_ingestao={data_ingestao}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_suffix = datetime.now(timezone.utc).strftime("%H%M%S")
    output_path = partition_dir / f"eventos_{file_suffix}.parquet"
    pd.DataFrame(events).to_parquet(output_path, index=False)
    logger.info("%d eventos salvos em %s", len(events), output_path)


if __name__ == "__main__":
    main()
