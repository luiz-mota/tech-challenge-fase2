import pandas as pd

from src.silver.quality_checks import (
    check_consistency,
    check_duplicates,
    check_nulls,
    check_uf_key_validity,
)


def test_check_duplicates_detects_repeated_key():
    df = pd.DataFrame({"id_municipio": ["1100015", "1100015"], "ano": [2023, 2023]})
    result = check_duplicates(df, ["id_municipio", "ano"])
    assert result["severity"] == "Critico"
    assert result["affected_count"] == 2


def test_check_duplicates_passes_with_unique_key():
    df = pd.DataFrame({"id_municipio": ["1100015", "1100023"], "ano": [2023, 2023]})
    result = check_duplicates(df, ["id_municipio", "ano"])
    assert result["severity"] is None


def test_check_nulls_critical_column_flags_any_null():
    df = pd.DataFrame({"id_municipio": ["1100015", None]})
    result = check_nulls(df, "id_municipio", critical=True)
    assert result["severity"] == "Critico"
    assert result["affected_count"] == 1


def test_check_nulls_non_critical_below_threshold_passes():
    df = pd.DataFrame({"taxa_alfabetizacao": [50.0, 60.0, None] + [70.0] * 97})
    result = check_nulls(df, "taxa_alfabetizacao", critical=False, threshold_pct=5.0)
    assert result["severity"] is None


def test_check_uf_key_validity_flags_unmapped_codes():
    df = pd.DataFrame({"sigla_uf": ["SP", None]})
    result = check_uf_key_validity(df)
    assert result["severity"] == "Critico"
    assert result["affected_count"] == 1


def test_check_consistency_flags_large_divergence():
    df = pd.DataFrame({
        "taxa_alfabetizacao": [80.0, 50.0],
        "taxa_alfabetizacao_alunos": [79.0, 20.0],
    })
    result = check_consistency(df, "taxa_alfabetizacao", "taxa_alfabetizacao_alunos", threshold=10.0)
    assert result["severity"] == "Alerta"
    assert result["affected_count"] == 1
