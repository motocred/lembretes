# main.py
import os
import functions_framework
from google.auth import default
from dotenv import load_dotenv
from flask import Request, Response, jsonify

from clouds import choose_audio_link
from send_messages import send_pos_venda, send_closes, send_atras
from filter_customers import format_df, seperate_customers_reminders, seperate_customers_atras, separete_customers_pos_sell
from clouds import read_sheet

load_dotenv()

SHEET_URL = os.getenv("SHEET_URL")

API_URL = os.getenv("API_URL")
CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")
TEST_PHONE = os.getenv("TEST_PHONE")

# Constants to format the dataframe
SELL_COLS = ["LIN", "COD_VENDA", "NOM_CLIENTE", "DOC_CLIENTE", "VALOR", "N_PARC", "DATA_REF", "MODELO", "VARIÁVEL", "PARC_CAR", "CARTÃO", "PARC_INI", "VLR_PARC", "REF_PARC_1"]
PARCS_COLS = ["ID", "COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "CARTÃO"]
ATRAS_COLS = ["NOME", "COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "Dias de atraso", "Comentários"]

@functions_framework.http
def main(request: Request) -> tuple[Response, int]:
    try:
        if not SHEET_URL:
            raise ValueError("Sheet url could not be loaded")

        credentials, _ = default()
        wallet_df = read_sheet(credentials, SHEET_URL, "CARTEIRA")
        sellings_df = read_sheet(credentials, SHEET_URL, "VENDAS")
        parcs_df = read_sheet(credentials, SHEET_URL, "PARCELAS")
        info_op_df = read_sheet(credentials, SHEET_URL, "INFO OPERACIONAL")
        atras_df = read_sheet(credentials, SHEET_URL, "ATRASO")

        # """
        # For local test:
        # import pandas as pd
        # wallet_df = pd.read_csv('../static/Carteira Motocred atualizada - CARTEIRA.csv')
        # sellings_df = pd.read_csv('../static/Carteira Motocred atualizada - VENDAS.csv')
        # parcs_df = pd.read_csv('../static/Carteira Motocred atualizada - PARCELAS.csv')
        # info_op_df = pd.read_csv('../static/Carteira Motocred atualizada - INFO OPERACIONAL.csv')
        # atras_df = pd.read_csv('../static/Carteira Motocred atualizada - ATRASO.csv')
        # """

        atras_df_formated = format_df(atras_df, ATRAS_COLS)
        parcs_df_formated = format_df(parcs_df, PARCS_COLS)
        selling_df_formated = format_df(sellings_df, SELL_COLS)

        DFS = {
            "wallet": wallet_df,
            "sellings": selling_df_formated,
            "parcs": parcs_df_formated,
            "info_op": info_op_df,
            "atras": atras_df_formated
        }

        SENDERS = {
            "pos": send_pos_venda,
            "closes": send_closes,
            "atras": send_atras
        }

        customers_erro = list()
        customers_pos = separete_customers_pos_sell(DFS, customers_erro)
        customers_reminders = seperate_customers_reminders(DFS, customers_erro)
        customers_atras = seperate_customers_atras(DFS, customers_erro)

        if not API_URL or not CLIENT_TOKEN:
            raise ValueError("Either api url or client token was not loaded")

        for group, audio_type in [(customers_pos, "pos"), (customers_reminders, "closes"), (customers_atras, "atras")]:
            for customer in group:
                message_data = {
                    "name": customer["name"], # Not necessary, but will keep it for futures implementations
                    "phone": customer["phone"],
                }
                audio = choose_audio_link(audio_type, customer["sex"]) # pyright: ignore

                SENDERS[audio_type](API_URL, CLIENT_TOKEN, message_data, audio)

        return jsonify({"message": "Teste concluído"}), 200
    except Exception as e:
        return jsonify({"Some error ocurred": str(e)}), 500
