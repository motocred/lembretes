# main.py
import os
import functions_framework
from google.auth import default
from dotenv import load_dotenv
from flask import Request, Response, jsonify

from clouds import choose_audio_link
from send_messages import send_pos, send_reminders, send_reminders_newers, send_atras
from customers_seperate import format_df, seperate_customers_reminders, seperate_customers_atras, separete_customers_pos_sell, seperate_customers_reminders_newers
from clouds import read_sheet

load_dotenv()

SHEET_URL = os.getenv("SHEET_URL")

API_URL = os.getenv("API_URL")
CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")
TEST_PHONE = os.getenv("TEST_PHONE")

# Constants to format the dataframe
SELL_COLS = ["COD_VENDA", "NOM_CLIENTE", "DOC_CLIENTE", "VALOR", "N_PARC", "DATA_REF", "MODELO", "VARIÁVEL", "PARC_CAR", "CARTÃO", "PARC_INI", "VLR_PARC", "REF_PARC_1"]
PARCS_COLS = ["COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "CARTÃO"]
ATRAS_COLS = ["NOME", "COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "Dias de atraso", "Comentários"]

@functions_framework.http
def main(request: Request) -> tuple[Response, int]:
    try:
        if not SHEET_URL:
            raise ValueError("Sheet url could not be loaded")

        # credentials, _ = default()
        # sellings_df = read_sheet(credentials, SHEET_URL, "VENDAS")
        # parcs_df = read_sheet(credentials, SHEET_URL, "PARCELAS")
        # info_op_df = read_sheet(credentials, SHEET_URL, "INFO OPERACIONAL")
        # atras_df = read_sheet(credentials, SHEET_URL, "ATRASO")

        # """
        # For local test:
        import pandas as pd
        sellings_df = pd.read_csv('../static/Carteira Motocred atualizada - VENDAS.csv')
        parcs_df = pd.read_csv('../static/Carteira Motocred atualizada - PARCELAS.csv')
        info_op_df = pd.read_csv('../static/Carteira Motocred atualizada - INFO OPERACIONAL.csv')
        atras_df = pd.read_csv('../static/Carteira Motocred atualizada - ATRASO.csv')
        # """

        atras_df_formated = format_df(atras_df, ATRAS_COLS)
        parcs_df_formated = format_df(parcs_df, PARCS_COLS)
        selling_df_formated = format_df(sellings_df, SELL_COLS)

        # TELEFONE column from info_op might come as float
        info_op_df['TELEFONE'] = info_op_df['TELEFONE'].apply(lambda x: str(int(x)) if pd.notna(x) else pd.NA)

        # Drop Cliente não encontrando lines
        atras_df_formated = atras_df_formated[atras_df_formated["NOME"] != "Cliente não encontrado"]

        DFS = {
            "sellings": selling_df_formated,
            "parcs": parcs_df_formated,
            "info_op": info_op_df,
            "atras": atras_df_formated
        }

        SENDERS = {
            "pos": send_pos,
            "reminder": send_reminders,
            "reminder_newer": send_reminders_newers,
            "atras": send_atras
        }

        customers_erro = list()
        customers_pos = separete_customers_pos_sell(DFS, customers_erro)
        customers_reminders = seperate_customers_reminders(DFS, customers_erro)
        customers_reminders_newer = seperate_customers_reminders_newers(DFS, customers_erro)
        # customers_atras = seperate_customers_atras(DFS, customers_erro) # While not ready

        if not API_URL or not CLIENT_TOKEN:
            raise ValueError("Either api url or client token was not loaded")

        for group, audio_type in [
            (customers_pos, "pos"),
            (customers_reminders, "reminders"),
            (customers_reminders_newer, "reminders_newers"), # Add atras
        ]:
            for customer in group:
                message_data = {
                    "name": customer["name"], # Not necessary, but will keep it for futures implementations
                    # "phone": customer["phone"],
                    "phone": TEST_PHONE,
                }
                audio_data = choose_audio_link(audio_type, customer["sex"]) # pyright: ignore

                SENDERS[audio_type](API_URL, CLIENT_TOKEN, message_data, audio_data)

        return jsonify({"message": "Teste concluído"}), 200
    except Exception as e:
        return jsonify({"Some error ocurred": str(e)}), 500
