import send_messages_utils as su

# DONE
def send_pos(url: str, token: str, customer_data: su.Payload, audios_data: dict[str, str]) -> None:
    payload_data = dict()
    payload_data.update(customer_data)

    payload_data["audio"] = audios_data["boas_vindas"]
    su.send_audio(url, token, payload_data)

    payload_data["audio"] = audios_data["perguntar_moto"]
    su.send_audio(url, token, payload_data)

    payload_data["audio"] = audios_data["usar_link"]
    su.send_audio(url, token, payload_data)

    payload_data["message"] = "https://cobranca.altis.online/" # Payment link
    su.send_text(url, token, payload_data)

def send_reminders(url: str, token: str, customer_data: su.Payload, text_data: str) -> None:
    payload_data = dict()
    payload_data.update(customer_data)
    payload_data["message"] = text_data
    su.send_text(url, token, payload_data)

def send_reminders_newers(url: str, token: str, customer_data: su.Payload, audio_data: str) -> None:
    return None # For now while no audio
    customer_data["message"] = audio_link
    su.send_audio(url, token, customer_data)

def send_atras(url: str, token: str, customer_data: su.Payload, audio_data: str) -> None:
    return None # For now while no audio
    customer_data["audio"] = audio_link
    su.send_audio(url, token, customer_data)
