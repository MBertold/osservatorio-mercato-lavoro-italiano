import pandas as pd

from src.eurostat_extract import _aggregate_by_territorio


def test_aggregate_merges_bolzano_and_trento_into_trentino_alto_adige():
    df = pd.DataFrame(
        {
            "geo": ["ITH1", "ITH2", "ITC4"],
            "freq": ["A", "A", "A"],
            "sex": ["T", "T", "T"],
            "age": ["Y15-64", "Y15-64", "Y15-64"],
            "TIME_PERIOD": [2023, 2023, 2023],
            "OBS_VALUE": [70.0, 74.0, 72.0],
        }
    )

    result = _aggregate_by_territorio(df)

    trentino = result[result["Territorio"] == "Trentino-Alto Adige"]
    assert len(trentino) == 1
    assert trentino["OBS_VALUE"].iloc[0] == 72.0  # media di 70 e 74

    lombardia = result[result["Territorio"] == "Lombardia"]
    assert lombardia["OBS_VALUE"].iloc[0] == 72.0


def test_aggregate_drops_non_italian_regions():
    df = pd.DataFrame(
        {
            "geo": ["AT", "AT1", "ITC4"],
            "freq": ["A", "A", "A"],
            "sex": ["T", "T", "T"],
            "age": ["Y15-64", "Y15-64", "Y15-64"],
            "TIME_PERIOD": [2023, 2023, 2023],
            "OBS_VALUE": [10.0, 11.0, 72.0],
        }
    )

    result = _aggregate_by_territorio(df)

    assert set(result["Territorio"]) == {"Lombardia"}


def test_aggregate_output_has_both_code_and_label_columns():
    df = pd.DataFrame(
        {
            "geo": ["ITC4"],
            "freq": ["A"],
            "sex": ["T"],
            "age": ["Y15-64"],
            "TIME_PERIOD": [2023],
            "OBS_VALUE": [72.0],
        }
    )

    result = _aggregate_by_territorio(df)

    assert result["geo"].iloc[0] == "Lombardia"
    assert result["Territorio"].iloc[0] == "Lombardia"
