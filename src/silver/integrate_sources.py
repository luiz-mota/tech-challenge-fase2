import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.utils.gcs_helpers import upload_file
from src.utils.logging_config import setup_logger

load_dotenv()

logger = setup_logger(__name__)

BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
SILVER_LOCAL_DIR = Path(__file__).resolve().parents[2] / "data" / "silver_local"

# Códigos oficiais de UF do IBGE (2 primeiros dígitos do código de município).
UF_CODE_TO_SIGLA = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


def load_silver_table(table_id: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER_LOCAL_DIR / f"{table_id}.parquet")


def build_municipio_meta(municipio: pd.DataFrame, meta_municipio: pd.DataFrame) -> pd.DataFrame:
    # meta_alfabetizacao_municipio.rede é sempre 'Municipal' (texto), enquanto
    # municipio.rede usa códigos numéricos — vocabulários incompatíveis, então o
    # join usa só id_municipio + ano (meta_alfabetizacao_municipio não tem
    # granularidade por rede: é sempre 1 linha por município/ano).
    meta_cols = [
        "id_municipio", "ano",
        "meta_alfabetizacao_2024", "meta_alfabetizacao_2025", "meta_alfabetizacao_2026",
        "meta_alfabetizacao_2027", "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
        "meta_alfabetizacao_2030", "nivel_alfabetizacao", "percentual_participacao",
    ]
    merged = municipio.merge(
        meta_municipio[meta_cols],
        on=["id_municipio", "ano"],
        how="left",
    )
    merged["sigla_uf"] = merged["id_municipio"].str[:2].map(UF_CODE_TO_SIGLA)
    return merged


def aggregate_alunos(alunos: pd.DataFrame) -> pd.DataFrame:
    avaliados = alunos[alunos["presenca"] == "1"]
    agg = avaliados.groupby(["id_municipio", "ano", "rede"]).agg(
        n_alunos_avaliados=("id_aluno", "count"),
        taxa_alfabetizacao_alunos=("alfabetizado", lambda s: (s == "1").mean() * 100),
        proficiencia_media=("proficiencia", "mean"),
    ).reset_index()
    return agg


def main() -> None:
    municipio = load_silver_table("municipio")
    meta_municipio = load_silver_table("meta_alfabetizacao_municipio")
    alunos = load_silver_table("alunos")

    fato = build_municipio_meta(municipio, meta_municipio)
    alunos_agg = aggregate_alunos(alunos)
    fato = fato.merge(alunos_agg, on=["id_municipio", "ano", "rede"], how="left")

    output_path = SILVER_LOCAL_DIR / "fato_alfabetizacao_municipio.parquet"
    fato.to_parquet(output_path, index=False)
    upload_file(BUCKET_NAME, output_path, "silver/fato_alfabetizacao_municipio.parquet")
    logger.info("Tabela integrada: %d linhas -> %s (e GCS silver/)", len(fato), output_path)


if __name__ == "__main__":
    main()
