"""
agents/portals_agent.py — Verifica presença em portais imobiliários brasileiros.
Portais: ZAP Imóveis, Viva Real, OLX, Chaves na Mão, Quinto Andar.
"""

import re
import time
import requests
from urllib.parse import quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

PORTAIS = {
    "ZAP": {
        "nome": "ZAP Imóveis",
        "busca_url": "https://www.zapimoveis.com.br/busca/?q={query}",
        "search_url": "https://www.zapimoveis.com.br/resultado/?q={query}",
    },
    "VivaReal": {
        "nome": "Viva Real",
        "busca_url": "https://www.vivareal.com.br/venda/{cidade}/?q={query}",
        "search_url": "https://www.vivareal.com.br/busca/?q={query}",
    },
    "OLX": {
        "nome": "OLX",
        "busca_url": "https://www.olx.com.br/imoveis?q={query}",
        "search_url": "https://www.olx.com.br/brasil?q={query}",
    },
    "ChavesNaMao": {
        "nome": "Chaves na Mão",
        "busca_url": "https://www.chavesnamao.com.br/imoveis-para-venda-e-locacao/?q={query}",
        "search_url": "https://www.chavesnamao.com.br/imoveis/?q={query}",
    },
    "QuintoAndar": {
        "nome": "Quinto Andar",
        "busca_url": "https://www.quintoandar.com.br/buscar/?q={query}",
        "search_url": "https://www.quintoandar.com.br/alugar/imovel/{cidade}",
    },
}


def verificar_portais(nome_empresa: str, cidade: str, site_url: str = None) -> dict:
    """
    Verifica presença da imobiliária em todos os portais.
    Retorna:
    {
        "portais_encontrados": ["ZAP", "VivaReal", ...],
        "detalhes": { "ZAP": {"encontrado": True, "url": "..."}, ... }
    }
    """
    # Estratégia principal: Google Search para cada portal
    # É mais confiável que scraping direto dos portais (que bloqueiam bots)
    portais_encontrados = []
    detalhes = {}

    for portal_id, portal_info in PORTAIS.items():
        time.sleep(0.8)
        encontrado, url_encontrada = _verificar_via_google(
            nome_empresa, cidade, portal_info["nome"], portal_id
        )
        detalhes[portal_id] = {
            "nome": portal_info["nome"],
            "encontrado": encontrado,
            "url": url_encontrada,
        }
        if encontrado:
            portais_encontrados.append(portal_info["nome"])

    return {
        "portais_encontrados": portais_encontrados,
        "detalhes": detalhes,
    }


def _verificar_via_google(nome: str, cidade: str, portal_nome: str, portal_id: str) -> tuple[bool, str | None]:
    """
    Usa Google para checar se a imobiliária tem página no portal.
    Query: site:zapimoveis.com.br "nome da imobiliária" "cidade"
    """
    dominio_mapa = {
        "ZAP": "zapimoveis.com.br",
        "VivaReal": "vivareal.com.br",
        "OLX": "olx.com.br",
        "ChavesNaMao": "chavesnamao.com.br",
        "QuintoAndar": "quintoandar.com.br",
    }
    dominio = dominio_mapa.get(portal_id, "")

    # Remove termos genéricos do nome para busca mais precisa
    nome_limpo = re.sub(r"\b(imóveis|imoveis|imobiliária|imobiliaria|brasil|corretor)\b",
                        "", nome, flags=re.IGNORECASE).strip()

    query = f'site:{dominio} "{nome_limpo}" "{cidade}"'
    url = f"https://www.google.com.br/search?q={quote(query)}&gl=br&hl=pt-BR"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return False, None

        html = r.text

        # Sem resultados
        nao_encontrado = any(x in html for x in [
            "Nenhum resultado encontrado",
            "não retornou nenhum documento",
            "did not match any documents",
            f"0 resultados para",
        ])
        if nao_encontrado:
            return False, None

        # Extrai primeira URL do portal nos resultados
        pattern = rf'https?://(?:www\.)?{re.escape(dominio)}/[^\s"<>]+'
        matches = re.findall(pattern, html)
        if matches:
            return True, matches[0]

        # Verifica se ao menos aparece o domínio nos resultados
        if dominio in html and nome_limpo.lower()[:5] in html.lower():
            return True, f"https://www.{dominio}"

        return False, None

    except Exception:
        return False, None


def resumo_portais(portais_encontrados: list) -> str:
    """Retorna string formatada para exibição."""
    if not portais_encontrados:
        return "Nenhum portal"
    return ", ".join(portais_encontrados)
