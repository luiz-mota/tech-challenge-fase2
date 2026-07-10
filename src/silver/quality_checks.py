import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.utils.gcs_helpers import upload_file
from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
SILVER_LOCAL_DIR = Path(__file__).resolve().parents[2] / "data" / "silver_local"
QUARANTINE_DIR = SILVER_LOCAL_DIR / "quarentena"

CRITICO = "Critico"
ALERTA = "Alerta"


def check_duplicates(df: pd.DataFrame, key_columns: list[str]) -> dict:
    duplicate_groups = df.duplicated(subset=key_columns, keep=False).sum()
    return {
        "check": "duplicidade_chave",
        "severity": CRITICO if duplicate_groups > 0 else None,
        "description": f"Linhas duplicadas na chave {key_columns}",
        "affected_count": int(duplicate_groups),
    }


def check_nulls(df: pd.DataFrame, column: str, critical: bool, threshold_pct: float = 5.0) -> dict:
    null_count = int(df[column].isna().sum())
    null_pct = (null_count / len(df) * 100) if len(df) else 0.0

    severity = None
    if critical and null_count > 0:
        severity = CRITICO
    elif null_pct > threshold_pct:
        severity = ALERTA

    return {
        "check": f"valores_ausentes_{column}",
        "severity": severity,
        "description": f"{null_pct:.2f}% de nulos na coluna '{column}'",
        "affected_count": null_count,
    }


def check_uf_key_validity(df: pd.DataFrame) -> dict:
    invalid_count = int(df["sigla_uf"].isna().sum())
    return {
        "check": "chave_uf_invalida",
        "severity": CRITICO if invalid_count > 0 else None,
        "description": "Municípios cujo código IBGE não mapeia para uma UF válida",
        "affected_count": invalid_count,
    }


def check_consistency(df: pd.DataFrame, col_a: str, col_b: str, threshold: float = 10.0) -> dict:
    comparable = df.dropna(subset=[col_a, col_b])
    diverging = (comparable[col_a] - comparable[col_b]).abs() > threshold
    return {
        "check": "consistencia_taxa_alfabetizacao",
        "severity": ALERTA if diverging.sum() > 0 else None,
        "description": f"Diferença > {threshold} pontos entre '{col_a}' (oficial) e '{col_b}' (agregado de alunos)",
        "affected_count": int(diverging.sum()),
    }


def run_all_checks(fato: pd.DataFrame) -> pd.DataFrame:
    findings = [
        check_duplicates(fato, ["id_municipio", "ano", "rede"]),
        check_nulls(fato, "id_municipio", critical=True),
        check_nulls(fato, "ano", critical=True),
        check_nulls(fato, "taxa_alfabetizacao", critical=False),
        check_nulls(fato, "meta_alfabetizacao_2024", critical=False),
        check_uf_key_validity(fato),
        check_consistency(fato, "taxa_alfabetizacao", "taxa_alfabetizacao_alunos"),
    ]

    log_df = pd.DataFrame(findings)
    log_df["checked_at"] = datetime.now(timezone.utc).isoformat()
    return log_df


def main() -> None:
    fato = pd.read_parquet(SILVER_LOCAL_DIR / "fato_alfabetizacao_municipio.parquet")
    log_df = run_all_checks(fato)

    for _, row in log_df.iterrows():
        if row["severity"]:
            logger.info("[%s] %s: %s", row["severity"], row["check"], row["description"])

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = QUARANTINE_DIR / f"quality_log_{timestamp}.parquet"
    log_df.to_parquet(output_path, index=False)
    upload_file(BUCKET_NAME, output_path, f"silver/quarentena/quality_log_{timestamp}.parquet")
    logger.info("Log de qualidade salvo em %s (e GCS silver/quarentena/)", output_path)


if __name__ == "__main__":
    main()
