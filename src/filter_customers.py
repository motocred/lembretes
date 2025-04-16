# filter_customers.py
import re
import datetime as dt
import pandas as pd

from errors import PhoneFormatError

def _get_sell_code(df_sellings: pd.DataFrame, line: int) -> str:
    return str(df_sellings["COD_VENDA"][line])

def _get_name_by_code(df_sellings: pd.DataFrame, sell_code: str) -> str:
    return df_sellings.loc[df_sellings["COD_VENDA"] == sell_code, "NOM_CLIENTE"].values[0]

def _get_phone_by_name(df_info_op: pd.DataFrame, name: str) -> str | None:
    df_info_op["CLIENTE"] = df_info_op["CLIENTE"].str.strip().str.upper()
    match_row = df_info_op.loc[df_info_op["CLIENTE"] == name.upper().strip(), "TELEFONE"]

    if not match_row.empty:
        return match_row.values[0]
    return None

def _get_sex_by_name(df_info_op: pd.DataFrame, name: str) -> str:
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


def _get_next_venc_by_sell_code(df_parcs: pd.DataFrame, sell_code: str) -> dt.date | None:
    sell_code_mask = (df_parcs["COD_VENDA"] == sell_code)
    df_parcs_sell_code = df_parcs.loc[sell_code_mask].copy()

    df_parcs_sell_code["DATA_PARC"] = pd.to_datetime(
        df_parcs_sell_code["DATA_PARC"].str.strip(),
        format="%d/%m/%y",
        dayfirst=True,
        errors="coerce",
    )

    prazo_mask = (df_parcs_sell_code["VERIF"].str.upper().str.strip() == "NO PRAZO")
    valid_dates = df_parcs_sell_code.loc[prazo_mask, "DATA_PARC"].dropna()

    if not valid_dates.empty:
        return valid_dates.min().date()
    return None

def format_df(df_raw: pd.DataFrame, expected_cols: list[str]) -> pd.DataFrame:
    for row_idx in range(len(df_raw)):
        row = df_raw.iloc[row_idx]
        for col_idx in range(len(row)):
            sub_row = row.iloc[col_idx:]
            if list(sub_row.values[:len(expected_cols)]) == expected_cols:
                # Slice out the relevant part of the dataframe
                df_trimmed = df_raw.iloc[row_idx + 1:, col_idx:col_idx + len(expected_cols)]
                df_trimmed.columns = expected_cols
                df_trimmed.reset_index(drop=True, inplace=True)
                return df_trimmed

    raise ValueError("Expected column names not found in the sheet.")

def format_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)

    if digits.startswith("55") and len(digits) == 13:
        return digits  # Already valid: 55DD9XXXXXXXX

    if len(digits) == 9 and digits.startswith("9"):
        # Just phone: 9XXXXXXXXX → assume DDD=84
        return "5584" + digits

    if len(digits) == 11 and digits[2] == "9":
        # DD9XXXXXXXX → just add 55
        return "55" + digits

    if len(digits) == 13 and not digits.startswith("55"):
        # Something like 84991234567 but extra stuff before
        return "55" + digits

    raise PhoneFormatError(f"Invalid phone phone format: {phone}")

def seperate_customers_reminders(dfs: dict[str, pd.DataFrame], customers_error: list[dict[str, str]]) -> list[dict[str, str]]:
    customers_reminders = list()
    today_date = dt.date.today()

    df_sellings = dfs["sellings"]
    df_parcs = dfs["parcs"]
    df_info_op = dfs["info_op"]

    for line in range(len(df_sellings)):
        # Get nex to venc
        sell_code = _get_sell_code(df_sellings, line)
        try:
            date_venc = _get_next_venc_by_sell_code(df_parcs, sell_code)
            if not date_venc:
                continue

            name = _get_name_by_code(df_sellings, sell_code)
            phone = _get_phone_by_name(df_info_op, name)
            sex = _get_sex_by_name(df_info_op, name)
            days_diff = (date_venc - today_date).days

            if days_diff in [10, 1, 0]: # Add to closes
                close = {
                    "name": name,
                    "sex": sex,
                    "phone": phone,
                    "days_to": days_diff,
                    "sell_code": sell_code
                }
                customers_reminders.append(close)
        except Exception as e:
            customer_error = {
                "sell_code": sell_code,
                "error": e,
                "type": "Reminder"
            }
            customers_error.append(customer_error)

    return customers_reminders


def seperate_customers_atras(dfs: dict[str, pd.DataFrame], customers_error: list[dict[str, str]]) -> list[dict[str, str]]:
    customers_atras = list()

    df_atras = dfs["atras"]
    df_info_op = dfs["info_op"]

    for _, row in df_atras.iterrows():
        sell_code = str(row["COD_VENDA"]).strip()
        try:
            if pd.isna(row["Dias de atraso"]):
                continue
            days = int(row["Dias de atraso"])
            name = str(row["NOME"]).strip()
            phone = _get_phone_by_name(df_info_op, name)
            sex = _get_sex_by_name(df_info_op, name)
            if days > 0 and days <= 3: # Add to atra
                atra = {
                    "name": name,
                    "sex": sex,
                    "phone": phone,
                    "days_after": days,
                    "sell_code": sell_code,
                }
                customers_atras.append(atra)
        except Exception as e:
            customer_error = {
                "sell_code": sell_code,
                "error": e,
                "type": "Atras"
            }
            customers_error.append(customer_error)

    return customers_atras

def separete_customers_pos_sell(dfs: dict[str, pd.DataFrame], customers_error: list[dict[str, str]]) -> list[dict[str, str]]:
    customers_pos_sell = list()
    today = dt.date.today()

    df_wallet = dfs["wallet"]
    df_info_op = dfs["info_op"]

    df_wallet["Data"] = pd.to_datetime(df_wallet["Data"], format="%d-%m-%Y")

    for _, row in df_wallet.iterrows():
        sell_code = row["Operação BMP"]
        try:
            date_sell = row["Data"].date()
            if pd.isna(date_sell):
                continue
            days = (today - date_sell).days
            if days == 3:
                name = str(row["Cliente"])
                phone = _get_phone_by_name(df_info_op, name)
                sex = _get_sex_by_name(df_info_op, name)

                data = {
                    "name": name,
                    "phone": phone,
                    "sell_code": sell_code,
                    "sex": sex,
                }

                customers_pos_sell.append(data)
        except Exception as e:
            customer_error = {
                "sell_code": sell_code,
                "error": e,
                "type": "Pos"
            }
            customers_error.append(customer_error)

    return customers_pos_sell

ATRAS_COLS = ["NOME", "COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "Dias de atraso", "Comentários"]
PARCS_COLS = ["ID", "COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "CARTÃO"]
SELL_COLS = ["LIN", "COD_VENDA", "NOM_CLIENTE", "DOC_CLIENTE", "VALOR", "N_PARC", "DATA_REF", "MODELO", "VARIÁVEL", "PARC_CAR", "CARTÃO", "PARC_INI", "VLR_PARC", "REF_PARC_1"]
