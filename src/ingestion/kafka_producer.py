import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "indicador_alfabetizacao"
MUNICIPIOS_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "municipio.parquet"
EVENT_TYPES = ["nova_medicao", "atualizacao_indicador"]


def load_municipio_ids() -> list[str]:
    df = pd.read_parquet(MUNICIPIOS_PATH, columns=["id_municipio"])
    return df["id_municipio"].unique().tolist()


def build_event(municipio_ids: list[str]) -> dict:
    return {
        "evento": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "id_municipio": random.choice(municipio_ids),
        "ano": random.choice([2023, 2024, 2025]),
        "taxa_alfabetizacao": round(random.uniform(40.0, 95.0), 2),
    }


def main(n_events: int = 50, interval_seconds: float = 0.5) -> None:
    municipio_ids = load_municipio_ids()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(2, 8, 2),
    )

    for i in range(n_events):
        event = build_event(municipio_ids)
        producer.send(TOPIC, value=event)
        logger.info("Evento %d/%d enviado: %s", i + 1, n_events, event)
        time.sleep(interval_seconds)

    producer.flush()
    producer.close()
    logger.info("%d eventos publicados no tópico '%s'", n_events, TOPIC)


if __name__ == "__main__":
    main()
