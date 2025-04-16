# clouds.py
import random
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe
from typing import Any, Literal

def read_sheet(credentials: Any, sheet_url: str, page: str) -> pd.DataFrame:
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_url(sheet_url)
    worksheet = spreadsheet.worksheet(page)
    df = get_as_dataframe(worksheet, evaluate_formulas=True)

    return df

def choose_audio_link(
    link_type: Literal["pos", "closes", "atras"],
    sex: Literal["M", "F"]
) -> str | dict[str, str]: # pyright: ignore
    sex_char = sex.lower()
    url_prefix = f"https://github.com/synthbe/motocred/raw/refs/heads/main/vendas/audios/{sex_char}" # Same for all

    if link_type == "pos":
        audio_index = random.randint(1, 3)
        boas_vindas = f"{url_prefix}/pos_venda/boas_vinda/boas_vindas_{audio_index}.m4a" # There are 3 audis

        audio_index = random.randint(1, 2)
        perguntar_moto = f"{url_prefix}/pos_venda/perguntar_moto/perguntar_moto_{audio_index}.m4a" # There are 2 audios

        audio_index = random.randint(1, 2)
        usar_link = f"{url_prefix}/pos_venda/usar_link/usar_link_{audio_index}.m4a" # There are 2 audios

        return { "boas_vindas": boas_vindas, "perguntar_moto": perguntar_moto, "usar_link": usar_link }
