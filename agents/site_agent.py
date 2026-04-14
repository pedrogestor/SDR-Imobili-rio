"""
agents/site_agent.py — Verifica se o site da imobiliária está no ar.
"""

import requests
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def verificar_site(url: str) -> dict:
    """
    Verifica se o site está funcional.
    Retorna:
    {
        "funcional": bool,
        "url_final": str,      # URL após redirecionamentos
        "status_code": int,
        "motivo": str
    }
    """
    if not url or url.lower() in ("não tem", "nao tem", "destivado", "desativado", "n/a", ""):
        return {"funcional": False, "url_final": None, "status_code": None, "motivo": "Sem site"}

    url_norm = _normalizar_url(url)
    if not url_norm:
        return {"funcional": False, "url_final": None, "status_code": None, "motivo": "URL inválida"}

    try:
        r = requests.get(url_norm, headers=HEADERS, timeout=12,
                         allow_redirects=True, stream=True)
        r.close()

        funcional = r.status_code < 400
        motivo = "" if funcional else f"HTTP {r.status_code}"

        # Verifica se caiu em página de "domínio à venda" ou parked domain
        if funcional and r.headers.get("content-type", "").startswith("text"):
            # Lê só os primeiros 5KB
            content = b""
            for chunk in r.iter_content(chunk_size=1024):
                content += chunk
                if len(content) > 5120:
                    break
            text = content.decode("utf-8", errors="ignore").lower()
            parked_signals = [
                "domain for sale", "domínio à venda", "buy this domain",
                "this domain is parked", "godaddy", "sedoparking",
                "this site can't be reached",
            ]
            if any(s in text for s in parked_signals):
                return {"funcional": False, "url_final": r.url,
                        "status_code": r.status_code, "motivo": "Domínio parked/à venda"}

        return {
            "funcional": funcional,
            "url_final": r.url,
            "status_code": r.status_code,
            "motivo": motivo,
        }

    except requests.Timeout:
        return {"funcional": False, "url_final": url_norm, "status_code": None, "motivo": "Timeout"}
    except requests.ConnectionError:
        return {"funcional": False, "url_final": url_norm, "status_code": None, "motivo": "Conexão recusada"}
    except Exception as e:
        return {"funcional": False, "url_final": url_norm, "status_code": None, "motivo": str(e)[:60]}


def _normalizar_url(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url
    # Valida básico
    if re.match(r"https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", url):
        return url
    return None
