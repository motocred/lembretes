# customers_seperate.py
import datetime as dt
import pandas as pd

import customers_utils as cu

TODAYS_DATE = dt.date.today()

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
                df_trimmed.dropna(how="all", inplace=True)
                return df_trimmed

    raise ValueError("Expected column names not found in the sheet.")

def seperate_customers_reminders_newers(dfs: dict[str, pd.DataFrame], errors_data: list[dict[str, str]]) -> list[dict[str, str]]:
    customer_list = list()
    df_wallet = dfs["wallet"]
    df_parcs = dfs["parcs"]
    df_info_op = dfs["info_op"]

    for line in range(len(df_wallet)):
        try:
            sell_code = cu.get_sell_code_by_line(df_wallet, line)
            sell_date = cu.get_sell_date_by_line(df_wallet, line)
            name = cu.get_name_by_line(df_wallet, line)
            next_venc = cu.get_next_venc_by_sell_code(df_parcs, sell_code)

            if next_venc is None: # Case when customer already quited
                continue
            if ((next_venc - sell_date).days // 30) > 3:
                continue
            if (TODAYS_DATE - next_venc).days != 10:
                continue

            sex = cu.get_sex_by_name(df_info_op, name)
            raw_phone = cu.get_phone_by_name(df_info_op, name)
            phone = cu.format_phone(raw_phone)

            customer_data = cu.create_customer_data(sell_code, name, phone=phone, sex=sex, sell_date=str(sell_date), group="Reminder_newer")
            customer_list.append(customer_data)

        except Exception as e:
            error_data = {
                "Error": str(e),
                "Type": "Reminder Newer",
                "Line": str(line)
            }
            errors_data.append(error_data)

    return customer_list

def seperate_customers_reminders(dfs: dict[str, pd.DataFrame], errors_data: list[dict[str, str]]) -> list[dict[str, str]]:
    customer_list = list()
    df_wallet = dfs["wallet"]
    df_parcs = dfs["parcs"]
    df_info_op = dfs["info_op"]

    for line in range(len(df_wallet)):
        try:
            sell_code = cu.get_sell_code_by_line(df_wallet, line)
            sell_date = cu.get_sell_date_by_line(df_wallet, line)
            name = cu.get_name_by_line(df_wallet, line)
            next_venc = cu.get_next_venc_by_sell_code(df_parcs, sell_code)

            if next_venc is None: # Case when customer already quited
                continue
            if ((next_venc - sell_date).days // 30) <= 3: # Belongs to newers
                continue
            if (TODAYS_DATE - next_venc).days != 1:
                continue

            sex = cu.get_sex_by_name(df_info_op, name)
            raw_phone = cu.get_phone_by_name(df_info_op, name)
            phone = cu.format_phone(raw_phone)

            customer_data = cu.create_customer_data(sell_code, name, phone, sex, next_venc=str(next_venc), group="Reminder")
            customer_list.append(customer_data)

        except Exception as e:
            error_data = {
                "Error": str(e),
                "Type": "Reminder",
                "Line": str(line)
            }
            errors_data.append(error_data)

    return customer_list

def seperate_customers_atras(dfs: dict[str, pd.DataFrame], errors_list: list[dict[str, str]]) -> list[dict[str, str]]:
    customers_list = list()
    df_atras = dfs["atras"]
    df_info_op = dfs["info_op"]

    pass

def separete_customers_pos_sell(dfs: dict[str, pd.DataFrame], errors_list: list[dict[str, str]]) -> list[dict[str, str]]:
    customers_list = list()
    df_wallet = dfs["wallet"]
    df_info_op = dfs["info_op"]

    for line in range(len(df_wallet)):
        try:
            sell_code = cu.get_sell_code_by_line(df_wallet, line)
            sell_date = cu.get_sell_date_by_line(df_wallet, line)
            name = cu.get_name_by_line(df_wallet, line)

            if (TODAYS_DATE - sell_date).days != 3:
                continue

            sex = cu.get_sex_by_name(df_info_op, name)
            raw_phone = cu.get_phone_by_name(df_info_op, name)
            phone = cu.format_phone(raw_phone)

            customer_data = cu.create_customer_data(sell_code, name, phone, sex, sell_date=str(sell_date), group="Pos")
            customers_list.append(customer_data)

        except Exception as e:
            error_data = {
                "Error": str(e),
                "Type": "Pos",
                "Line": str(line)
            }
            errors_list.append(error_data)

    return customers_list
