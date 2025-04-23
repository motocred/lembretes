import send_messages_utils as su

# DONE
def send_pos(url: str, token: str, message_data: su.Payload, audios_data: dict[str, str]) -> None:
    message_data["audio"] = audios_data["boas_vindas"]
    su.send_audio(url, token, message_data)

    message_data["audio"] = audios_data["perguntar_moto"]
    su.send_audio(url, token, message_data)

    message_data["audio"] = audios_data["usar_link"]
    su.send_audio(url, token, message_data)

    message_data["message"] = "https://cobranca.altis.online/" # Payment link
    su.send_text(url, token, message_data)

def send_reminders(url: str, token: str, message_data: su.Payload, audio_data: str) -> None:
    return None # For now while no audio
    message_data["message"] = audio_link
    su.send_audio(url, token, message_data)

def send_reminders_newers(url: str, token: str, message_data: su.Payload, audio_data: str) -> None:
    return None # For now while no audio
    message_data["message"] = audio_link
    su.send_audio(url, token, message_data)

def send_atras(url: str, token: str, message_data: su.Payload, audio_data: str) -> None:
    return None # For now while no audio
    message_data["audio"] = audio_link
    su.send_audio(url, token, message_data)
