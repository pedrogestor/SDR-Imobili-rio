"""
agents/ads_agent.py — Verificação de anúncios Meta (Ads Library) e Google.
Meta: via API pública da Ads Library (sem autenticação para anúncios de imóveis).
Google: scraping do Google Search para detectar anúncios pagos.
"""

import re
import time
import requests
from urllib.parse import quote, urlparse

HEADERS_DESKTOP = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Limites de descarte
LIMITE_META = 8
LIMITE_GOOGLE = 15


def verificar_meta_ads(nome_empresa: str, site_url: str = None, meta_token: str = None) -> dict:
    """
    Verifica anúncios ativos no Meta Ads Library.
    Usa a API pública (não precisa de token para anúncios não-políticos).
    
    Retorna:
    {
        "anuncia": bool,
        "quantidade": int,
        "descartado": bool,   # True se > LIMITE_META anúncios ativos
        "anuncios": list,     # Amostra dos anúncios encontrados
        "erro": str | None
    }
    """
    # Método 1: API oficial com token (se fornecido)
    if meta_token:
        resultado = _meta_api_oficial(nome_empresa, meta_token)
        if "erro" not in resultado:
            return resultado

    # Método 2: Meta Ads Library pública (scraping)
    resultado = _meta_ads_scraping(nome_empresa, site_url)
    return resultado


def _meta_api_oficial(nome: str, token: str) -> dict:
    """Usa a Graph API da Meta para buscar anúncios."""
    try:
        # Busca o page_id pelo nome da empresa
        search_url = (
            f"https://graph.facebook.com/v19.0/search"
            f"?q={quote(nome)}&type=page&access_token={token}"
        )
        r = requests.get(search_url, timeout=10)
        if r.status_code != 200:
            return {"erro": f"Meta Graph API: HTTP {r.status_code}"}

        pages = r.json().get("data", [])
        if not pages:
            return {"anuncia": False, "quantidade": 0, "descartado": False, "anuncios": []}

        page_id = pages[0]["id"]

        # Busca anúncios ativos dessa página
        ads_url = (
            f"https://graph.facebook.com/v19.0/ads_archive"
            f"?access_token={token}"
            f"&ad_reached_countries=['BR']"
            f"&search_page_ids={page_id}"
            f"&ad_active_status=ACTIVE"
            f"&fields=id,ad_creative_bodies,ad_delivery_start_time"
            f"&limit=50"
        )
        r2 = requests.get(ads_url, timeout=10)
        if r2.status_code != 200:
            return {"erro": f"Ads Archive: HTTP {r2.status_code}"}

        ads = r2.json().get("data", [])
        qtd = len(ads)
        return {
            "anuncia": qtd > 0,
            "quantidade": qtd,
            "descartado": qtd > LIMITE_META,
            "anuncios": ads[:3],
            "erro": None,
        }
    except Exception as e:
        return {"erro": str(e)}


def _meta_ads_scraping(nome: str, site_url: str = None) -> dict:
    """
    Scraping da Meta Ads Library pública.
    URL: https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=BR&q=NOME
    """
    try:
        query = quote(nome)
        url = (
            f"https://www.facebook.com/ads/library/?"
            f"active_status=active&ad_type=all&country=BR&q={query}&media_type=all"
        )
        r = requests.get(url, headers=HEADERS_DESKTOP, timeout=15)

        if r.status_code != 200:
            return _resultado_meta(False, 0, f"HTTP {r.status_code}")

        html = r.text

        # Detecta se há anúncios via padrões no HTML
        # A Ads Library carrega via React, então buscamos sinais no HTML inicial
        sem_anuncios = any(x in html for x in [
            "Não encontramos anúncios",
            "No ads found",
            "no_ads",
            '"totalCount":0',
        ])

        if sem_anuncios:
            return _resultado_meta(False, 0, None)

        # Conta padrões que indicam anúncios
        count_patterns = len(re.findall(r'"ad_archive_id"', html))

        # Se não achou padrões diretos, verifica pelo domínio do site
        if count_patterns == 0 and site_url:
            dominio = _extrair_dominio(site_url)
            if dominio:
                url2 = (
                    f"https://www.facebook.com/ads/library/?"
                    f"active_status=active&ad_type=all&country=BR"
                    f"&search_type=page&view_all_page_id=&q={quote(dominio)}"
                )
                r2 = requests.get(url2, headers=HEADERS_DESKTOP, timeout=15)
                if r2.status_code == 200:
                    count_patterns = len(re.findall(r'"ad_archive_id"', r2.text))

        tem_ads = count_patterns > 0
        return _resultado_meta(tem_ads, count_patterns, None)

    except Exception as e:
        return _resultado_meta(False, 0, f"Erro: {str(e)[:80]}")


def verificar_google_ads(nome_empresa: str, cidade: str, site_url: str = None) -> dict:
    """
    Verifica presença de anúncios Google Ads para a empresa.
    Método: busca no Google por "[nome] [cidade]" e detecta anúncios pagos.

    Retorna:
    {
        "anuncia": bool,
        "quantidade": int,      # estimativa baseada nos resultados
        "descartado": bool,
        "tipos": list,          # ex: ["pesquisa", "display"]
        "erro": str | None
    }
    """
    try:
        query = f"{nome_empresa} {cidade} imóveis"
        url = f"https://www.google.com.br/search?q={quote(query)}&gl=br&hl=pt-BR"

        headers = {**HEADERS_DESKTOP, "Accept-Encoding": "gzip, deflate"}
        r = requests.get(url, headers=headers, timeout=15)

        if r.status_code != 200:
            return _resultado_google(False, 0, [], f"HTTP {r.status_code}")

        html = r.text
        qtd, tipos = _detectar_ads_google(html, nome_empresa, site_url)
        descartado = qtd > LIMITE_GOOGLE

        # Segunda busca: verifica se o site aparece em anúncios de display
        if site_url and qtd == 0:
            dominio = _extrair_dominio(site_url)
            if dominio:
                url2 = f"https://www.google.com.br/search?q=site:{dominio}&gl=br&hl=pt-BR"
                time.sleep(1)
                r2 = requests.get(url2, headers=headers, timeout=15)
                if r2.status_code == 200:
                    qtd2, tipos2 = _detectar_ads_google(r2.text, nome_empresa, site_url)
                    if qtd2 > qtd:
                        qtd, tipos = qtd2, tipos2

        return _resultado_google(qtd > 0, qtd, tipos, None)

    except Exception as e:
        return _resultado_google(False, 0, [], f"Erro: {str(e)[:80]}")


def _detectar_ads_google(html: str, nome: str, site_url: str = None) -> tuple[int, list]:
    """Detecta anúncios do Google no HTML da SERP."""
    tipos = []
    count = 0

    # Indicadores de anúncios na SERP do Google
    indicadores_ad = [
        r'data-text-ad',
        r'<span[^>]*>Patrocinado</span>',
        r'<span[^>]*>Anúncio</span>',
        r'"Sponsored"',
        r'class="[^"]*uEierd[^"]*"',   # classe CSS de ad label do Google
        r'class="[^"]*vdQmEd[^"]*"',
    ]

    for ind in indicadores_ad:
        matches = len(re.findall(ind, html, re.IGNORECASE))
        count += matches
        if matches > 0:
            if "pesquisa" not in tipos:
                tipos.append("pesquisa")

    # Verifica Google Shopping / Display
    if re.search(r'class="[^"]*mnr-c[^"]*"', html):
        tipos.append("shopping")

    # Verifica se o domínio da empresa aparece em contexto de ad
    if site_url:
        dominio = _extrair_dominio(site_url)
        if dominio:
            padrao_ad_dominio = rf'({re.escape(dominio)})[^\n]*(?:Patrocinado|Anúncio|Sponsored)'
            if re.search(padrao_ad_dominio, html, re.IGNORECASE):
                count = max(count, 1)
                if "pesquisa" not in tipos:
                    tipos.append("pesquisa")

    return count, tipos


def _extrair_dominio(url: str) -> str | None:
    try:
        parsed = urlparse(url if url.startswith("http") else "https://" + url)
        domain = parsed.netloc.replace("www.", "")
        return domain if domain else None
    except Exception:
        return None


def _resultado_meta(anuncia, quantidade, erro):
    return {
        "anuncia": anuncia,
        "quantidade": quantidade,
        "descartado": quantidade > LIMITE_META,
        "anuncios": [],
        "erro": erro,
    }


def _resultado_google(anuncia, quantidade, tipos, erro):
    return {
        "anuncia": anuncia,
        "quantidade": quantidade,
        "descartado": quantidade > LIMITE_GOOGLE,
        "tipos": tipos,
        "erro": erro,
    }


def determinar_abordagem(meta: dict, google: dict) -> str:
    """
    Determina qual abordagem usar (A, B ou C) baseado nos resultados de ads.
    Lógica: se anuncia nos dois, prioriza o de maior presença.
    """
    anuncia_meta = meta.get("anuncia", False) and not meta.get("descartado", False)
    anuncia_google = google.get("anuncia", False) and not google.get("descartado", False)

    if not anuncia_meta and not anuncia_google:
        return "A"  # Padrão

    if anuncia_google and anuncia_meta:
        # Prioriza o de maior volume
        if google.get("quantidade", 0) >= meta.get("quantidade", 0):
            return "B"  # Google
        return "C"  # Meta

    if anuncia_google:
        return "B"

    return "C"  # Meta
