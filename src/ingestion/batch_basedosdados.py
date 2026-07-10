from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

from src.utils.gcs_helpers import get_credentials
from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BILLING_PROJECT_ID = "tech-challenge-fase2-501722"
QUERY_PROJECT_ID = "basedosdados"
DATASET_ID = "br_inep_avaliacao_alfabetizacao"
TABLE_IDS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

# Preço on-demand do BigQuery além do free tier de 1 TB/mês (us/southamerica-east1).
PRICE_PER_TIB_USD = 6.25


def _build_client() -> bigquery.Client:
    credentials = get_credentials(["https://www.googleapis.com/auth/bigquery"])
    return bigquery.Client(credentials=credentials, project=BILLING_PROJECT_ID)


def _estimate_cost(client: bigquery.Client, query: str) -> int:
    dry_run_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(query, job_config=dry_run_config)
    bytes_processed = job.total_bytes_processed
    tib_processed = bytes_processed / (1024**4)
    estimated_usd = tib_processed * PRICE_PER_TIB_USD
    logger.info(
        "Estimativa: %.2f MB a processar (~US$ %.4f além do free tier de 1 TB/mês)",
        bytes_processed / (1024**2),
        estimated_usd,
    )
    return bytes_processed


def extract_table(client: bigquery.Client, table_id: str) -> None:
    query = f"SELECT * FROM `{QUERY_PROJECT_ID}.{DATASET_ID}.{table_id}`"
    logger.info("Extraindo %s.%s", DATASET_ID, table_id)
    _estimate_cost(client, query)

    df = client.query(query).to_dataframe()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{table_id}.parquet"
    df.to_parquet(output_path, index=False)
    logger.info("%s: %d linhas salvas em %s", table_id, len(df), output_path)


def main() -> None:
    client = _build_client()
    for table_id in TABLE_IDS:
        extract_table(client, table_id)


if __name__ == "__main__":
    main()
