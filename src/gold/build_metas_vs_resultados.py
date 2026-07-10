import pandas as pd

from src.gold._common import REDE_MUNICIPAL, load_fato_municipio
from src.utils.bq_helpers import upload_dataframe
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)

DATASET_ID = "gold"
TABLE_ID = "metas_vs_resultados"
META_COLUMNS = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]


def build_metas_vs_resultados(fato: pd.DataFrame) -> pd.DataFrame:
    municipal = fato[fato["rede"] == REDE_MUNICIPAL]
    latest = municipal.sort_values("ano").groupby("id_municipio", as_index=False).tail(1)

    cols = ["id_municipio", "sigla_uf", "ano", "taxa_alfabetizacao"] + META_COLUMNS
    result = latest[cols].copy()
    result["gap_meta_2030"] = result["meta_alfabetizacao_2030"] - result["taxa_alfabetizacao"]
    return result


def main() -> None:
    fato = load_fato_municipio()
    metas_vs_resultados = build_metas_vs_resultados(fato)
    upload_dataframe(metas_vs_resultados, DATASET_ID, TABLE_ID)
    logger.info("%s: %d linhas -> BigQuery %s.%s", TABLE_ID, len(metas_vs_resultados), DATASET_ID, TABLE_ID)


if __name__ == "__main__":
    main()
