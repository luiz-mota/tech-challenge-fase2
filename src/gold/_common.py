import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.utils.gcs_helpers import download_file

load_dotenv()

SILVER_LOCAL_DIR = Path(__file__).resolve().parents[2] / "data" / "silver_local"

# Compromisso Nacional Criança Alfabetizada mede a rede Municipal. Confirmado
# empiricamente: rede='3' é a que bate com meta_alfabetizacao_municipio
# (diferença média de 0.017 ponto, vs. 14.7 para rede='2').
REDE_MUNICIPAL = "3"


def load_fato_municipio() -> pd.DataFrame:
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    local_path = SILVER_LOCAL_DIR / "fato_alfabetizacao_municipio.parquet"
    download_file(bucket_name, "silver/fato_alfabetizacao_municipio.parquet", local_path)
    return pd.read_parquet(local_path)
