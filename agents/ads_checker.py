"""
agents/ads_checker.py - Verificacao de anuncios pagos.
Google Ads: Selenium no Google Ads Transparency Center (logica validada).
Meta Ads: pendente de solucao definitiva (retorna nao_verificado por enquanto).
"""

import re
import time
import requests
from urllib.parse import quote, urlparse
from dataclasses import dataclass, field

TIMEOUT = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


@dataclass
class ResultadoAds:
    # Meta
    meta_ads_active:         bool = False
    meta_ads_count_estimate: int  = 0
    meta_ads_source:         str  = None
    meta_ads_status:         str  = "nao_verificado"
    # verificado | nao_encontrado | nao_verificado | falha

    # Google
    google_ads_active:         bool = False
    google_ads_count_estimate: int  = 0
    google_ads_source:         str  = None
    google_ads_status:         str  = "nao_verificado"
    # verificado | inconclusivo | captcha | bloqueado | erro | nao_verificado

    # Consolidado
    dominant_channel:    str  = None   # Meta | Google | Ambos | Nenhum
    confidence_score:    int  = 0      # 0-10
    ads_summary:         str  = None
    verification_status: str  = "pendente"
    error_message:       str  = None
    raw_debug:           dict = field(default_factory=dict)


# ── Helper de dominio ─────────────────────────────────────────────────────────

def _dominio(url: str):
    try:
        d = urlparse(url if url.startswith("http") else "https://"+url).netloc
        return d.lower().replace("www.", "") or None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE ADS - logica validada (2/2 acertos em teste real)
# ══════════════════════════════════════════════════════════════════════════════

def verificar_google_ads(driver, nome: str, cidade: str,
                          site_url: str = None, log=None) -> dict:
    """
    Verifica anuncios no Google Ads Transparency Center pelo dominio do site.
    Padrao confirmado: texto "N anuncios" no HTML renderizado.
    Sem site = nao_verificado (nao ha como buscar sem dominio).
    """
    def _log(msg):
        if log:
            log(f"  [GOOGLE] {msg}")

    resultado = {
        "google_ads_active":         False,
        "google_ads_count_estimate": 0,
        "google_ads_source":         "google_ads_transparency_center",
        "google_ads_status":         "nao_verificado",
        "google_raw":                {},
    }

    dominio = _dominio(site_url) if site_url else None
    if not dominio:
        _log("Sem site — nao verificado")
        resultado["google_raw"]["motivo"] = "sem_site_para_buscar"
        return resultado

    resultado["google_raw"]["dominio"] = dominio
    _log(f"Verificando dominio: {dominio}")

    url = "https://adstransparency.google.com/?region=BR&domain=" + quote(dominio)
    try:
        driver.get(url)
        time.sleep(6)
        driver.execute_script("window.scrollTo(0, 300)")
        time.sleep(0.5)
        html = driver.page_source

        if "captcha" in html.lower():
            resultado["google_ads_status"] = "captcha"
            _log("CAPTCHA detectado")
            return resultado

        if "accounts.google.com" in driver.current_url:
            resultado["google_ads_status"] = "bloqueado_login"
            _log("Bloqueado - redirecionou para login")
            return resultado

        # Padrao validado: "8 anuncios" ou "0 anuncios"
        padrao_pt = re.compile(r'(\d+)\s+an\u00FAncio', re.IGNORECASE)
        padrao_en = re.compile(r'(\d+)\s+ad\b', re.IGNORECASE)

        m = padrao_pt.search(html) or padrao_en.search(html)
        if m:
            n = int(m.group(1))
            resultado["google_ads_active"]         = n > 0
            resultado["google_ads_count_estimate"] = n
            resultado["google_ads_status"]         = "verificado"
            resultado["google_raw"]["ads_count"]   = n
            if n > 0:
                _log(f"Anuncia no Google - {n} anuncio(s)")
            else:
                _log("Nao anuncia no Google")
        else:
            resultado["google_ads_status"] = "inconclusivo"
            _log("Inconclusivo - padrao nao encontrado no HTML")

        return resultado

    except Exception as e:
        resultado["google_ads_status"] = "erro_selenium"
        resultado["google_raw"]["erro"] = str(e)[:100]
        _log(f"Erro: {str(e)[:80]}")
        return resultado


# ══════════════════════════════════════════════════════════════════════════════
# META ADS - pendente de solucao definitiva
# Opcoes em avaliacao:
#   1. Selenium no autocomplete da Ads Library (dificuldade no dropdown)
#   2. Aprovacao do app na Meta Ads Library API
# Por enquanto retorna nao_verificado sem bloquear o pipeline.
# ══════════════════════════════════════════════════════════════════════════════

def verificar_meta_ads(driver, nome: str, cidade: str,
                        site_url: str = None, log=None) -> dict:
    """
    Verificacao Meta Ads ainda em implementacao.
    Retorna nao_verificado sem erro para nao bloquear o pipeline.
    """
    def _log(msg):
        if log:
            log(f"  [META] {msg}")

    _log("Verificacao Meta pendente - retornando nao_verificado")
    return {
        "meta_ads_active":         False,
        "meta_ads_count_estimate": 0,
        "meta_ads_source":         None,
        "meta_ads_status":         "nao_verificado",
        "meta_raw":                {"motivo": "implementacao_pendente"},
    }


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR
# ══════════════════════════════════════════════════════════════════════════════

def verificar_anuncios(driver, nome: str, cidade: str,
                        site_url: str = None, log=None) -> ResultadoAds:
    """
    Verifica Meta Ads + Google Ads e consolida resultado.
    """
    def _log(msg):
        if log:
            log(msg)

    r = ResultadoAds()

    # Google Ads
    _log("Verificando Google Ads...")
    google = verificar_google_ads(driver, nome, cidade, site_url, log=log)
    r.google_ads_active         = google["google_ads_active"]
    r.google_ads_count_estimate = google["google_ads_count_estimate"]
    r.google_ads_source         = google["google_ads_source"]
    r.google_ads_status         = google["google_ads_status"]
    r.raw_debug["google"]       = google.get("google_raw", {})

    # Meta Ads
    _log("Verificando Meta Ads...")
    meta = verificar_meta_ads(driver, nome, cidade, site_url, log=log)
    r.meta_ads_active         = meta["meta_ads_active"]
    r.meta_ads_count_estimate = meta["meta_ads_count_estimate"]
    r.meta_ads_source         = meta["meta_ads_source"]
    r.meta_ads_status         = meta["meta_ads_status"]
    r.raw_debug["meta"]       = meta.get("meta_raw", {})

    # Consolidacao
    google_ok = r.google_ads_status == "verificado"
    meta_ok   = r.meta_ads_status   == "verificado"

    if r.meta_ads_active and r.google_ads_active:
        r.dominant_channel = "Ambos"
        r.confidence_score = 10
    elif r.google_ads_active:
        r.dominant_channel = "Google"
        r.confidence_score = 9 if google_ok else 5
    elif r.meta_ads_active:
        r.dominant_channel = "Meta"
        r.confidence_score = 9 if meta_ok else 5
    else:
        r.dominant_channel = "Nenhum"
        r.confidence_score = 8 if google_ok else 4

    # Status geral da verificacao
    if google_ok and meta_ok:
        r.verification_status = "completo"
    elif google_ok:
        r.verification_status = "parcial"  # Meta pendente
    else:
        r.verification_status = "falhou"

    # Resumo legivel
    partes = []
    if r.google_ads_active:
        partes.append(f"Google ({r.google_ads_count_estimate} anuncio(s))")
    if r.meta_ads_active:
        partes.append(f"Meta ({r.meta_ads_count_estimate} anuncio(s))")

    if partes:
        r.ads_summary = "Anuncia em: " + " e ".join(partes)
    elif google_ok:
        r.ads_summary = "Nao anuncia no Google"
    else:
        r.ads_summary = "Nao verificado"

    _log(f"Resultado: {r.ads_summary} | confianca={r.confidence_score}/10")
    return r
