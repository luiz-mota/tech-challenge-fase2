import pandas as pd

from src.gold._common import REDE_MUNICIPAL, load_fato_municipio
from src.utils.bq_helpers import upload_dataframe
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

DATASET_ID = "gold"
TABLE_ID = "evolucao_temporal"


def build_evolucao_temporal(fato: pd.DataFrame) -> pd.DataFrame:
    municipal = fato[fato["rede"] == REDE_MUNICIPAL]
    return municipal[[
        "id_municipio", "sigla_uf", "ano",
        "taxa_alfabetizacao", "n_alunos_avaliados",
    ]].sort_values(["id_municipio", "ano"]).copy()


def main() -> None:
    fato = load_fato_municipio()
    evolucao = build_evolucao_temporal(fato)
    upload_dataframe(evolucao, DATASET_ID, TABLE_ID)
    logger.info("%s: %d linhas -> BigQuery %s.%s", TABLE_ID, len(evolucao), DATASET_ID, TABLE_ID)


if __name__ == "__main__":
    main()
