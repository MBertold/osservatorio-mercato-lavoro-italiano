import pandas as pd
import pytest

from src.transform import (
    RIPARTIZIONE_PER_REGIONE,
    _parse_anno,
    _pick_column,
    normalize_indicator_df,
)


def test_pick_column_returns_first_match():
    df = pd.DataFrame({"REF_AREA": [1], "OBS_VALUE": [2]})
    assert _pick_column(df, ["ITTER107", "REF_AREA"]) == "REF_AREA"


def test_pick_column_raises_when_required_and_missing():
    df = pd.DataFrame({"FOO": [1]})
    with pytest.raises(KeyError):
        _pick_column(df, ["ITTER107", "REF_AREA"])


def test_pick_column_returns_none_when_optional_and_missing():
    df = pd.DataFrame({"FOO": [1]})
    assert _pick_column(df, ["SEXISTAT1"], required=False) is None


def test_parse_anno_extracts_year_from_various_periods():
    assert _parse_anno("2023") == 2023
    assert _parse_anno("2023-Q1") == 2023
    assert _parse_anno("2023-04") == 2023


def test_parse_anno_returns_none_for_malformed_period():
    assert _parse_anno("Q1-23") is None


def test_normalize_indicator_df_maps_sdmx_columns_to_long_schema():
    raw = pd.DataFrame(
        {
            "ITTER107": ["IT", "IT"],
            "Territorio": ["Italia", "Italia"],
            "TIME_PERIOD": ["2023-Q1", "2023-Q2"],
            "SEXISTAT1": ["1", "1"],
            "OBS_VALUE": ["7.5", "7.8"],
        }
    )

    clean = normalize_indicator_df(raw, "tasso_disoccupazione")

    assert list(clean["indicatore"].unique()) == ["tasso_disoccupazione"]
    assert clean["valore"].tolist() == [7.5, 7.8]
    assert clean["territorio"].tolist() == ["Italia", "Italia"]
    # ETA1 assente nel csv di esempio: deve ricadere sul default "totale"
    assert (clean["classe_eta"] == "totale").all()


def test_normalize_indicator_df_coerces_non_numeric_value_to_nan():
    raw = pd.DataFrame(
        {
            "REF_AREA": ["IT"],
            "TIME_PERIOD": ["2023"],
            "OBS_VALUE": ["n.d."],
        }
    )
    clean = normalize_indicator_df(raw, "tasso_occupazione")
    assert clean["valore"].isna().all()


def test_ripartizione_covers_all_twenty_regioni():
    assert len(RIPARTIZIONE_PER_REGIONE) == 20
    assert set(RIPARTIZIONE_PER_REGIONE.values()) == {"Nord", "Centro", "Sud"}
