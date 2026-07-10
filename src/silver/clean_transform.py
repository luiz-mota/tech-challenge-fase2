import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.utils.gcs_helpers import download_file, get_latest_partition, upload_file
from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
TABLE_IDS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]
BRONZE_LOCAL_DIR = Path(__file__).resolve().parents[2] / "data" / "bronze_local" / "batch"
SILVER_LOCAL_DIR = Path(__file__).resolve().parents[2] / "data" / "silver_local"

# Colunas-chave que identificam a granularidade de cada tabela (não podem ser nulas).
KEY_COLUMNS = {
    "uf": ["sigla_uf", "ano", "rede"],
    "municipio": ["id_municipio", "ano", "rede"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "ano", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "ano", "rede"],
    "alunos": ["id_aluno", "ano"],
}


def clean_table(df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    df = df.drop_duplicates()

    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype("string").str.zfill(7)
    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].astype("string").str.strip().str.upper()
    if "ano" in df.columns:
        df["ano"] = df["ano"].astype("Int64")

    key_columns = KEY_COLUMNS[table_id]
    before = len(df)
    df = df.dropna(subset=key_columns)
    dropped = before - len(df)
    if dropped:
        logger.info("%s: %d linhas descartadas por chave nula (%s)", table_id, dropped, key_columns)

    return df


def process_table(table_id: str, partition: str) -> None:
    blob_path = f"bronze/batch/{table_id}/{partition}/{table_id}.parquet"
    local_bronze_path = BRONZE_LOCAL_DIR / f"{table_id}.parquet"
    download_file(BUCKET_NAME, blob_path, local_bronze_path)

    df = pd.read_parquet(local_bronze_path)
    df_clean = clean_table(df, table_id)

    SILVER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    local_silver_path = SILVER_LOCAL_DIR / f"{table_id}.parquet"
    df_clean.to_parquet(local_silver_path, index=False)

    upload_file(BUCKET_NAME, local_silver_path, f"silver/{table_id}.parquet")
    logger.info("%s: %d linhas limpas -> %s (e GCS silver/)", table_id, len(df_clean), local_silver_path)


def main() -> None:
    partition = get_latest_partition(BUCKET_NAME, "bronze/batch/")
    logger.info("Usando partição de bronze: %s", partition)
    for table_id in TABLE_IDS:
        process_table(table_id, partition)


if __name__ == "__main__":
    main()
