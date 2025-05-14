# customers_utils.py
import re
import datetime as dt
import pandas as pd

from errors import PhoneFormatError

def get_sell_code_by_line(df_wallet: pd.DataFrame, line: int) -> str:
    sell_code = df_wallet.at[line, "Operação BMP"]
    if pd.isna(sell_code):
        raise ValueError("No sell code value")
    try:
        return str(int(float(sell_code)))
    except (ValueError, TypeError):
        raise ValueError(f"Invalid sell code: {sell_code}")

def get_sell_date_by_line(df_wallet: pd.DataFrame, line: int) -> dt.date:
    ts = pd.to_datetime(df_wallet.at[line, "Data"], dayfirst=True)
    return ts.date()

def get_name_by_line(df_wallet: pd.DataFrame, line: int) -> str:
    name = str(df_wallet["Cliente"][line])
    if pd.isna(name):
        raise ValueError(f"No name value")
    return name.upper().strip()

def get_name_by_code(df_sellings: pd.DataFrame, sell_code: str) -> str:
    name = df_sellings.loc[df_sellings["COD_VENDA"] == sell_code, "NOM_CLIENTE"].values[0]
    if pd.isna(name):
        raise ValueError(f"Name not found by sell code {sell_code}")
    return name


def get_phone_by_name(df_info_op: pd.DataFrame, name: str) -> str | None:
    df_info_op["CLIENTE"] = df_info_op["CLIENTE"].str.strip().str.upper()
    match_row = df_info_op.loc[df_info_op["CLIENTE"] == name.upper().strip(), "TELEFONE"]

    if match_row.empty:
        raise ValueError(f"Phone not found by name {name}")
    return match_row.values[0]

def get_sex_by_name(df_info_op: pd.DataFrame, name: str) -> str:
    df_info_op["CLIENTE"] = df_info_op["CLIENTE"].str.strip().str.upper()
    match_row = df_info_op.loc[df_info_op["CLIENTE"] == name.strip().upper(), "SEXO"]

    if match_row.empty:
        return "M"  # default

    raw_sex = match_row.values[0]
    if pd.isna(raw_sex):
        return "M"

    raw_sex = str(raw_sex).strip().lower()
    if raw_sex in {"f", "feminino", "mulher"}:
        return "F"
    elif raw_sex in {"m", "masculino", "homem"}:
        return "M"
    return "M"  # fallback padrão


def get_next_venc_by_sell_code(df_parcs: pd.DataFrame, sell_code: str) -> dt.date | None:
    sell_code = str(sell_code).strip()
    df_parcs_sell_code = df_parcs[
        df_parcs["COD_VENDA"].astype(str).str.strip() == sell_code
    ]

    if df_parcs_sell_code.empty:
        raise ValueError(f"Sell code {sell_code} not found in Parcelas")

    df_parcs_atras = df_parcs_sell_code[
        df_parcs_sell_code["VERIF"].astype(str).str.upper().str.strip() == "ATRASADO"
    ].copy()

    if not df_parcs_atras.empty:
        return None

    df_parcs_no_prazo = df_parcs_sell_code[
        df_parcs_sell_code["VERIF"].astype(str).str.upper().str.strip() == "NO PRAZO"
    ].copy()

    if df_parcs_no_prazo.empty:
        return None

    df_parcs_no_prazo["DATA_PARC"] = pd.to_datetime(
        df_parcs_no_prazo["DATA_PARC"].astype(str).str.strip(),
        format="%d/%m/%y",
        dayfirst=True,
        errors="coerce",
    )

    dates = df_parcs_no_prazo["DATA_PARC"].dropna()
    return dates.min().date() if not dates.empty else None

def get_atras_venc_by_sell_code(df_parcs: pd.DataFrame, sell_code: str) -> dt.date | None:
    sell_code = str(sell_code).strip()
    df_parcs_sell_code = df_parcs[
        df_parcs["COD_VENDA"].astype(str).str.strip() == sell_code
    ]

    if df_parcs_sell_code.empty:
        raise ValueError(f"Sell code {sell_code} not found in Parcelas")

    df_parcs_atras = df_parcs_sell_code[
        df_parcs_sell_code["VERIF"].astype(str).str.upper().str.strip() == "ATRASADO"
    ].copy()

    if df_parcs_atras.empty:
        return None

    df_parcs_atras["DATA_PARC"] = pd.to_datetime(
        df_parcs_atras["DATA_PARC"].astype(str).str.strip(),
        format="%d/%m/%y",
        dayfirst=True,
        errors="coerce",
    )

    dates = df_parcs_atras["DATA_PARC"].dropna()
    return dates.min().date() if not dates.empty else None

def format_phone(phone: str | None | float) -> str:
    if not isinstance(phone, str):
        raise PhoneFormatError(f"Invalid phone type: {phone}")

    digits = re.sub(r'\D', '', phone)
    if len(digits) < 8:
        raise ValueError("Invalid number: less than 8 digits")

    subscriber = digits[-8:]
    ddd = digits[:2]

    return f'55{ddd}9{subscriber}'

def create_customer_data(
        sell_code: str,
        name: str,
        phone: str="",
        sex: str="M",
        **kwargs: str
) -> dict[str, str]:
    data = {
        "sell_code": sell_code,
        "name": name,
        "phone": phone,
        "sex": sex,
    }
    data.update(kwargs)
    return data
