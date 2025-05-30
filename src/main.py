# main.py
import os
import re
import json
import functions_framework
import logging
from google.auth import default
from google.oauth2 import service_account
from dotenv import load_dotenv
from flask import Request, Response, jsonify
import pandas as pd

from clouds import choose_message, read_sheet
from send_messages import send_pos, send_reminders, send_reminders_newers, send_atras, send_reminders_today
from customers_seperate import format_df, seperate_customers_reminders, seperate_customers_atras, separete_customers_pos_sell, seperate_customers_reminders_newers, seperate_customers_reminders_today

load_dotenv()

SHEET_URL = os.getenv("SHEET_URL")

API_URL = os.getenv("API_URL")
CLIENT_TOKEN = os.getenv("CLIENT_TOKEN")
TEST_PHONE = os.getenv("TEST_PHONE")

SERVICE_ACCOUNT_FILE_PATH = os.getenv("SERVICE_ACCOUNT_FILE_PATH")

PARCS_COLS = ["COD_VENDA", "PARC", "DATA_PARC", "VLR_PARC", "VLR_PGTO", "VERIF", "CARTÃO"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)

@functions_framework.http
def main(request: Request) -> tuple[Response, int]:
    try:
        if not SHEET_URL:
            raise ValueError("Sheet url could not be loaded")

        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = None
        if SERVICE_ACCOUNT_FILE_PATH:
            credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE_PATH, scopes=scopes)
        else:
            credentials, _ = default()

        wallet_df = read_sheet(credentials, SHEET_URL, "CARTEIRA")
        parcs_df = read_sheet(credentials, SHEET_URL, "PARCELAS")
        info_op_df = read_sheet(credentials, SHEET_URL, "INFO OPERACIONAL")

        # TELEFONE column from info_op might come as float
        info_op_df['TELEFONE'] = info_op_df['TELEFONE'].apply(
            lambda x: re.sub(r'\D', '', str(x)) if pd.notna(x) else pd.NA
        )
        wallet_df = wallet_df[wallet_df["Status BMP"].str.upper().str.strip() != "CANCELADA"].reset_index()
        parcs_df_formated = format_df(parcs_df, PARCS_COLS)

        DFS = {
            "wallet": wallet_df,
            "parcs": parcs_df_formated,
            "info_op": info_op_df,
        }

        SENDERS = {
            "pos": send_pos,
            "reminders": send_reminders,
            "reminders_newers": send_reminders_newers,
            "reminders_today": send_reminders_today,
            "atras": send_atras
        }

        customers_erro = set()
        customers_pos = separete_customers_pos_sell(DFS, customers_erro)
        customers_reminders = seperate_customers_reminders(DFS, customers_erro)
        customers_reminders_newer = seperate_customers_reminders_newers(DFS, customers_erro)
        customers_atras = seperate_customers_atras(DFS, customers_erro)
        customers_reminders_today = seperate_customers_reminders_today(DFS, customers_erro)

        if not API_URL or not CLIENT_TOKEN:
            raise ValueError("Either api url or client token was not loaded")

        for group, message_type in [
            (customers_pos, "pos"),
            (customers_reminders, "reminders"),
            (customers_reminders_newer, "reminders_newers"),
            (customers_atras, "atras"),
            (customers_reminders_today, "reminders_today")
        ]:
            logging.info(f"{len(group)} customers of type {message_type}")
            for customer in group:
                customer_data = {
                    "phone": customer["phone"],
                }
                message_data = choose_message(message_type, customer["sex"]) # pyright: ignore

                logging.info(f"Sending message to {customer['name']} number {customer['phone']} of type {message_type}")
                SENDERS[message_type](API_URL, CLIENT_TOKEN, customer_data, message_data)
                logging.info(f"Success in sending message to {customer['name']}")

        for item in customers_erro:
            data = json.loads(item)
            logging.info(f"{data}")

        return jsonify({"message": "Success"}), 200
    except Exception as e:
        return jsonify({"Some error ocurred": str(e)}), 500
