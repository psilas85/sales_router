# routing_engine/services/whatsapp_service.py

from __future__ import annotations

import os
import re
from datetime import date
from typing import Optional

import requests
from loguru import logger


META_API_TOKEN = os.getenv("META_WHATSAPP_TOKEN", "")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID", "")
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0")

DIAS_SEMANA = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]


def _normalizar_numero(celular: str) -> Optional[str]:
    digits = re.sub(r"\D", "", celular)
    if len(digits) == 10:
        digits = digits[:2] + "9" + digits[2:]
    if len(digits) == 11:
        digits = "55" + digits
    if len(digits) != 13:
        return None
    return digits


def _build_message(
    consultor_nome: str,
    rota_data: date,
    visitas: list[dict],
    distancia_km: Optional[float],
) -> str:
    dia = DIAS_SEMANA[rota_data.weekday()]
    data_str = rota_data.strftime("%d/%m/%Y")
    primeiro_nome = consultor_nome.split()[0].title()

    lines = [
        f"🗓️ *Roteiro — {data_str} ({dia})*",
        f"",
        f"Olá, {primeiro_nome}! 👋 Aqui está seu roteiro de visitas:",
        f"",
    ]

    for v in visitas:
        nome = v.get("nome_fantasia") or v.get("razao_social") or v.get("cnpj") or "Cliente"
        partes_end = [
            v.get("logradouro", ""),
            v.get("numero", ""),
        ]
        complemento = " - ".join(filter(None, [
            ", ".join(filter(None, partes_end)),
            v.get("bairro", ""),
            f"{v.get('cidade', '')}/{v.get('uf', '')}",
        ]))

        lat = v.get("lat")
        lon = v.get("lon")
        maps_url = f"https://maps.google.com/maps?q={lat},{lon}" if lat and lon else None
        waze_url = f"https://waze.com/ul?ll={lat},{lon}&navigate=yes" if lat and lon else None

        lines.append(f"*{v['sequencia']}.* {nome}")
        if complemento:
            lines.append(f"📍 {complemento}")
        if maps_url:
            lines.append(f"🗺️ Maps: {maps_url}")
        if waze_url:
            lines.append(f"🧭 Waze: {waze_url}")
        lines.append("")

    km_str = f" | ~{distancia_km:.0f} km" if distancia_km else ""
    lines.append(f"---")
    lines.append(f"✅ *{len(visitas)} visita(s){km_str}*")
    lines.append(f"Bom trabalho! 💪")

    return "\n".join(lines)


def enviar_roteiro(
    consultor_nome: str,
    celular: str,
    rota_data: date,
    visitas: list[dict],
    distancia_km: Optional[float] = None,
    instancia: Optional[str] = None,
) -> dict:
    numero = _normalizar_numero(celular)
    if not numero:
        raise ValueError(f"Número inválido: {celular}")

    if not META_API_TOKEN or not META_PHONE_NUMBER_ID:
        raise RuntimeError("META_WHATSAPP_TOKEN e META_PHONE_NUMBER_ID não configurados")

    mensagem = _build_message(consultor_nome, rota_data, visitas, distancia_km)

    url = f"https://graph.facebook.com/{META_API_VERSION}/{META_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": numero,
        "type": "text",
        "text": {"body": mensagem},
    }

    logger.info(f"[WA] Enviando roteiro para {consultor_nome} ({numero})")
    response = requests.post(url, json=payload, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()
