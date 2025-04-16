import requests
from requests.exceptions import RequestException
from typing import TypeAlias

Payload: TypeAlias = dict[str, str | bool]

def _send_message(
    url_with_route: str,
    token: str,
    message_data: Payload,
    required_key: str,
    endpoint_suffix: str
) -> None:
    if not url_with_route.endswith(endpoint_suffix):
        raise ValueError(f"URL must end with '{endpoint_suffix}'")

    if required_key not in message_data:
        raise ValueError(f"message_data must contain '{required_key}'")

    data = {
        "phone": message_data["phone"],
        required_key: message_data[required_key]
    }

    headers = {"Client-Token": token}

    try:
        response = requests.post(url_with_route, data=data, headers=headers, timeout=15)
        response.raise_for_status()
    except RequestException as e:
        raise RequestException(f"Failed to send message to {data['phone']}: {str(e)}")

def _send_text(url: str, token: str, message_data: Payload) -> None:
    url_with_route = f"{url}/send-text"
    _send_message(url_with_route, token, message_data, "message", "send-text")

def _send_audio(url: str, token: str, message_data: Payload) -> None:
    url_with_route = f"{url}/send-audio"
    _send_message(url_with_route, token, message_data, "audio", "send-audio")

def send_pos_venda(url: str, token: str, message_data: Payload, audios_links: dict[str, str]) -> None:
    message_data["audio"] = audios_links["boas_vindas"]
    _send_audio(url, token, message_data)

    message_data["audio"] = audios_links["perguntar_moto"]
    _send_audio(url, token, message_data)

    message_data["audio"] = audios_links["usar_link"]
    _send_audio(url, token, message_data)

    message_data["message"] = "https://cobranca.altis.online/" # Payment link
    _send_text(url, token, message_data)

def send_closes(url: str, token: str, message_data: Payload, audio_link: str) -> None:
    return None # For now while no audio
    message_data["message"] = audio_link
    _send_audio(url, token, message_data)

def send_atras(url: str, token: str, message_data: Payload, audio_link: str) -> None:
    return None # For now while no audio
    message_data["audio"] = audio_link
    _send_audio(url, token, message_data)
