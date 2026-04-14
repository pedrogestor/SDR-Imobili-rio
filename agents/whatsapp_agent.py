"""
agents/whatsapp_agent.py — Validação de número e geração de link WhatsApp.
Usa a API não-oficial wa.me para checar se número tem WhatsApp.
"""

import re
import requests
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def normalizar_numero(telefone: str, ddd_padrao: str = None) -> str | None:
    """
    Normaliza número brasileiro para formato internacional: 5511999998888
    Aceita: 11999998888, (11)99999-8888, +55 11 99999-8888, etc.
    """
    limpo = re.sub(r"\D", "", telefone or "")

    if not limpo:
        return None

    # Remove prefixo 55 se já tiver
    if limpo.startswith("55") and len(limpo) > 11:
        limpo = limpo[2:]

    # Adiciona DDD se não tiver (número com 8 ou 9 dígitos)
    if len(limpo) in (8, 9) and ddd_padrao:
        ddd = re.sub(r"\D", "", ddd_padrao)[:2]
        limpo = ddd + limpo

    # Valida: deve ter 10 ou 11 dígitos (com DDD)
    if len(limpo) not in (10, 11):
        return None

    # Garante que celular com 9 dígitos tenha o 9 na frente
    if len(limpo) == 10:
        ddd = limpo[:2]
        num = limpo[2:]
        if len(num) == 8 and num[0] in "6789":
            limpo = ddd + "9" + num

    return "55" + limpo


def gerar_link_whatsapp(numero_normalizado: str) -> str:
    """Gera link direto para o WhatsApp Web."""
    return f"https://api.whatsapp.com/send/?phone={numero_normalizado}&type=phone_number&app_absent=0"


def verificar_whatsapp(numero: str) -> bool:
    """
    Verifica se o número tem WhatsApp consultando wa.me.
    Retorna True se o número for válido no WhatsApp.
    Nota: essa verificação é best-effort — pode ter falsos negativos.
    """
    try:
        url = f"https://wa.me/{numero}"
        r = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        # wa.me redireciona para página de chat se o número existir
        # Se retornar erro ou página de "número não encontrado", é inválido
        if r.status_code == 200:
            # Verifica se a página indica número válido
            invalido = any(x in r.text.lower() for x in [
                "phone number shared via url is invalid",
                "invalid phone number",
                "número de telefone inválido"
            ])
            return not invalido
        return False
    except Exception:
        return False  # Em caso de erro de rede, não descarta


def processar_telefones(telefones: list[str], cidade: str = None) -> dict:
    """
    Recebe lista de telefones brutos, normaliza e testa WhatsApp.
    Retorna:
    {
        "whatsapp_link": "https://...",  # melhor número com WA
        "whatsapp_validado": True/False,
        "telefones_normalizados": ["5511..."],
        "numero_whatsapp": "5511..."
    }
    """
    # Tenta extrair DDD da cidade se disponível
    ddd_padrao = _ddd_por_cidade(cidade) if cidade else None

    normalizados = []
    for t in telefones:
        n = normalizar_numero(t, ddd_padrao)
        if n and n not in normalizados:
            normalizados.append(n)

    # Prioriza celulares (11 dígitos com 9 na frente)
    celulares = [n for n in normalizados if len(n) == 13 and n[4] == "9"]
    fixos = [n for n in normalizados if n not in celulares]
    ordenados = celulares + fixos

    for numero in ordenados:
        time.sleep(0.3)  # gentil com o servidor
        tem_wpp = verificar_whatsapp(numero)
        if tem_wpp:
            return {
                "whatsapp_link": gerar_link_whatsapp(numero),
                "whatsapp_validado": True,
                "telefones_normalizados": normalizados,
                "numero_whatsapp": numero,
            }

    # Nenhum validado — retorna o primeiro com link mesmo assim
    if ordenados:
        return {
            "whatsapp_link": gerar_link_whatsapp(ordenados[0]),
            "whatsapp_validado": False,
            "telefones_normalizados": normalizados,
            "numero_whatsapp": ordenados[0],
        }

    return {
        "whatsapp_link": None,
        "whatsapp_validado": False,
        "telefones_normalizados": normalizados,
        "numero_whatsapp": None,
    }


def _ddd_por_cidade(cidade: str) -> str | None:
    """Mapa simplificado cidade → DDD para ajudar normalização."""
    mapa = {
        "são paulo": "11", "sp": "11", "campinas": "19", "santos": "13",
        "rio de janeiro": "21", "rj": "21", "niterói": "21",
        "belo horizonte": "31", "bh": "31", "contagem": "31", "betim": "31",
        "porto alegre": "51", "curitiba": "41", "florianópolis": "48",
        "salvador": "71", "fortaleza": "85", "recife": "81",
        "goiânia": "62", "manaus": "92", "belém": "91",
        "vitória": "27", "natal": "84", "joão pessoa": "83",
        "maceió": "82", "teresina": "86", "campo grande": "67",
        "cuiabá": "65", "macapá": "96", "porto velho": "69",
        "rio branco": "68", "palmas": "63", "boa vista": "95",
        "aracaju": "79", "são luís": "98",
        "americana": "19", "são josé do rio preto": "17",
        "ribeirão preto": "16", "sorocaba": "15", "osasco": "11",
        "santo andré": "11", "guarulhos": "11",
    }
    return mapa.get(cidade.lower().strip())
