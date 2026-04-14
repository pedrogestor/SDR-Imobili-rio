"""
agents/instagram_agent.py — Valida perfil Instagram de imobiliária.
Verifica: existe, é público, tem post nos últimos 56 dias (8 semanas).
Usa scraping leve via requests (sem Selenium para ser mais rápido).
"""

import re
import json
import time
import requests
from datetime import datetime, timedelta, timezone

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LIMITE_SEMANAS = 8
LIMITE_DIAS = LIMITE_SEMANAS * 7


def validar_perfil(instagram_url: str) -> dict:
    """
    Valida um perfil Instagram.
    Retorna:
    {
        "existe": bool,
        "ativo": bool,            # postou nos últimos 56 dias
        "ultimo_post": str|None,  # data ISO do último post detectado
        "username": str,
        "url_normalizada": str,
        "motivo": str             # motivo de inatividade/erro se houver
    }
    """
    username = _extrair_username(instagram_url)
    if not username:
        return _resultado(False, False, None, "", instagram_url, "URL inválida")

    url_norm = f"https://www.instagram.com/{username}/"

    try:
        time.sleep(0.5)
        r = requests.get(url_norm, headers=HEADERS, timeout=15)

        if r.status_code == 404:
            return _resultado(False, False, None, username, url_norm, "Perfil não encontrado")

        if r.status_code == 429:
            time.sleep(5)
            r = requests.get(url_norm, headers=HEADERS, timeout=15)

        if r.status_code != 200:
            return _resultado(True, None, None, username, url_norm, f"HTTP {r.status_code} — verificar manualmente")

        html = r.text

        # Perfil privado
        if '"is_private":true' in html or "Esta conta é privada" in html:
            return _resultado(True, False, None, username, url_norm, "Perfil privado")

        # Tenta extrair data do último post via JSON embutido na página
        ultimo_post = _extrair_ultimo_post(html)

        if ultimo_post:
            dias_atrás = (datetime.now(timezone.utc) - ultimo_post).days
            ativo = dias_atrás <= LIMITE_DIAS
            motivo = "" if ativo else f"Último post há {dias_atrás} dias (limite: {LIMITE_DIAS})"
            return _resultado(True, ativo, ultimo_post.strftime("%Y-%m-%d"), username, url_norm, motivo)

        # Não conseguiu detectar data — considera ativo se o perfil existe e tem conteúdo
        tem_posts = '"edge_owner_to_timeline_media":{"count":' in html
        if not tem_posts:
            return _resultado(True, False, None, username, url_norm, "Sem posts detectados")

        # Fallback: retorna como necessitando verificação manual
        return _resultado(True, None, None, username, url_norm, "Data do último post não detectada — verificar manualmente")

    except requests.Timeout:
        return _resultado(True, None, None, username, url_norm, "Timeout — verificar manualmente")
    except Exception as e:
        return _resultado(True, None, None, username, url_norm, f"Erro: {str(e)[:80]}")


def _extrair_ultimo_post(html: str) -> datetime | None:
    """Tenta extrair timestamp do último post do HTML do Instagram."""
    # Padrão 1: JSON-LD / SharedData
    patterns = [
        r'"taken_at_timestamp":(\d{10})',
        r'"date_gmt":"([^"]+)"',
        r'"uploadDate":"([^"]+)"',
        r'datetime="(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, html)
        if matches:
            try:
                val = matches[0]
                if val.isdigit():
                    ts = int(val)
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    # Tenta parse ISO
                    val_clean = val[:19].replace("T", " ")
                    dt = datetime.strptime(val_clean, "%Y-%m-%d %H:%M:%S")
                    return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def _extrair_username(url: str) -> str | None:
    """Extrai username de uma URL do Instagram."""
    if not url:
        return None
    # Remove parâmetros e trailing slash
    url = url.strip().rstrip("/").split("?")[0]

    # Se já é só um username (sem http)
    if not url.startswith("http"):
        return url.lstrip("@")

    # Extrai do path
    match = re.search(r"instagram\.com/([^/?#]+)", url)
    if match:
        username = match.group(1)
        # Ignora paths de explore, p/, reel/, stories/
        if username not in ("p", "reel", "stories", "explore", "tv", "accounts"):
            return username
    return None


def _resultado(existe, ativo, ultimo_post, username, url, motivo):
    return {
        "existe": existe,
        "ativo": ativo,
        "ultimo_post": ultimo_post,
        "username": username,
        "url_normalizada": url,
        "motivo": motivo,
    }


def buscar_imobiliarias_instagram(termo_busca: str, limite: int = 20) -> list[dict]:
    """
    Busca imobiliárias no Instagram por termo (ex: "imobiliária Florianópolis").
    Usa a busca interna do Instagram via API não-oficial.
    Retorna lista de dicts com username, nome, url.
    """
    results = []
    try:
        query = termo_busca.replace(" ", "%20")
        url = f"https://www.instagram.com/web/search/topsearch/?query={query}&context=blended"
        headers = {**HEADERS, "X-IG-App-ID": "936619743392459"}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            for item in data.get("users", [])[:limite]:
                user = item.get("user", {})
                nome = user.get("full_name", "")
                username = user.get("username", "")
                # Filtra por termos imobiliários
                termos_imob = ["imob", "imoveis", "imóveis", "realt", "corretor",
                               "casas", "apartamentos", "property", "homes"]
                nome_lower = nome.lower() + username.lower()
                if any(t in nome_lower for t in termos_imob):
                    results.append({
                        "nome": nome,
                        "username": username,
                        "url": f"https://www.instagram.com/{username}/",
                        "seguidores": user.get("follower_count", 0),
                    })
    except Exception:
        pass
    return results
