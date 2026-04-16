"""
agents/enrichment_agent.py — v12
Pipeline de resolução de identidade de marca para imobiliárias.

Arquitetura:
- Tokens de identidade distintos de tokens genéricos do setor
- Siglas curtas (LDL, K2, etc.) exigem match exato no handle/display/domínio
- Proteção contra ambiguidade: token principal ausente no perfil → bloqueio
- Site: 2 queries DDG + verificação em brand/body/aux/ig
- Instagram: fase 1 via requests (identidade), fase 2 Selenium (métricas)
- Data de posts: JSON embutido → posts individuais → DOM+scroll → JS
- Constantes de negócio centralizadas em config.py
"""

import re
import time
import random
import unicodedata
import requests
from datetime import date, datetime as _dt, timezone as _tz
from urllib.parse import urlparse, quote, parse_qs, unquote
from dataclasses import dataclass

from selenium.webdriver.common.by import By as _By
from selenium.webdriver.support.ui import WebDriverWait as _WDW
from selenium.webdriver.support import expected_conditions as _EC

from config import (
    MIN_SEGUIDORES_IG    as MIN_SEGUIDORES,
    MIN_POSTS_IG         as MIN_POSTS,
    MAX_SEMANAS_SEM_POST as MAX_SEMANAS_INATIVO,
    TIMEOUT_HTTP         as TIMEOUT_PAGINA,
    TIMEOUT_HTTP_RETRY   as TIMEOUT_RETRY,
)

# ── Critérios de tentativas ───────────────────────────────────────────────────
MAX_TENTATIVAS_SITE = 3
MAX_TENTATIVAS_IG   = 3

# ── Rate limiting DDG ─────────────────────────────────────────────────────────
_ddg_request_count = 0
_DELAY_BASE         = 3.5
_DELAY_JITTER       = 1.5
_DELAY_BLOQUEIO     = 40

# ── Cidades com nome ambíguo (precisam de UF na query) ───────────────────────
CIDADES_AMBIGUAS = {"saopaulo": "SP", "riodejaneiro": "RJ"}

# ── Portais e diretórios — nunca são site oficial ─────────────────────────────
PORTAIS = [
    "zapimoveis", "vivareal", "olx", "quintoandar", "chavesnamao",
    "imovelweb", "wimoveis", "netimoveis", "123imoveis", "imobiliare",
    "facebook", "linkedin", "twitter", "youtube", "tiktok",
    "google", "bing", "yahoo", "gstatic", "googleapis",
    "receitaws", "minhareceita", "econodata", "jusbrasil", "escavador",
    "solutudo", "telelista", "apontador", "guiamais", "listel",
    "procuroacho", "advdinamico", "empresaqui", "infobel",
    "serasaexperian", "serasa", "boavista", "spc", "reclameaqui",
]

DOMINIOS_TERCEIROS = [
    "cadastroempresa.com.br", "parafa.com.br", "cnpj.biz", "cnpja.com",
    "econodata.com.br", "infoinvest.com.br", "empresasdobrasil.com.br",
    "cnpjbrasil.com", "cnpjbrasil.com.br", "receitaws.com.br",
    "solutudo.com.br", "telelistas.net", "listaonline.com.br",
    "apontador.com.br", "guiamais.com.br", "listel.com.br",
    "infobel.com", "vriconsulting.com.br", "cylex.com.br", "encontra",
]

PATHS_TERCEIROS = [
    "/cnpj/", "/empresa/", "/empresas/", "/cadastro/", "/perfil/",
    "/fornecedor/", "/consulta-empresa/", "/consultar/", "/guia/",
    "/empresa.php", "/company/", "/companies/",
]

IGNORAR_IG = {
    "p", "reel", "reels", "stories", "explore", "tv", "accounts",
    "instagram", "about", "sharedfiles", "highlights",
}

TERMOS_IMOB = [
    "imoveis", "imobiliaria", "corretor", "corretora",
    "venda", "locacao", "aluguel", "apartamento", "casa", "terreno",
]

GENERIC_IDENTITY_TOKENS = {
    "imoveis", "imovel", "imobiliaria", "imobiliarias", "corretor", "corretora",
    "negocios", "empreendimentos", "administradora", "bens",
    "consultoria", "servicos", "grupo", "holding", "brasil",
    "imob", "imobiliario", "imobiliarios",
    "associados", "parceiros", "gestao", "investimentos",
}

STOPWORDS = {
    "ltda", "eireli", "me", "sa", "s", "a", "epp", "ss",
    "e", "de", "da", "do", "das", "dos", "em", "com", "para", "por", "o", "os", "as",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ResultadoEnriquecimento:
    nome:                str
    cidade:              str
    site_url:            str  = None
    site_confirmado:     bool = False
    site_score:          int  = 0
    site_sinais:         list = None
    site_motivo:         str  = None
    sem_site:            bool = False
    instagram_url:       str  = None
    ig_handle:           str  = None
    ig_confirmado:       bool = False
    ig_metodo:           str  = None
    ig_origem:           str  = None
    ig_reciproco:        bool = False
    ig_seguidores:       int  = None
    ig_num_posts:        int  = None
    ig_ultimo_post:      str  = None
    ig_semanas:          int  = None
    ig_motivo:           str  = None
    motivo_rejeicao_ig:  str  = None
    sem_instagram:       bool = False
    erro:                str  = None
    review_flags:        list = None

    def __post_init__(self):
        if self.site_sinais  is None: self.site_sinais  = []
        if self.review_flags is None: self.review_flags = []


# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZAÇÃO E TOKENS DE IDENTIDADE
# ══════════════════════════════════════════════════════════════════════════════

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in s if not unicodedata.combining(c))


def _cidade_query(cidade: str) -> str:
    uf = CIDADES_AMBIGUAS.get(_norm(cidade).replace(" ", ""))
    return f"{cidade} {uf}" if uf else cidade


def _cidade_e_ambigua(cidade: str) -> bool:
    return _norm(cidade).replace(" ", "") in CIDADES_AMBIGUAS


def _dominio_limpo(url: str) -> str:
    try:
        return urlparse(
            url if url.startswith("http") else f"https://{url}"
        ).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _url_raiz(url: str):
    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return None


def _raw_tokens(nome: str) -> list:
    return [p for p in re.split(r"\W+", _norm(nome)) if p]


def _tokens_identidade(nome: str) -> list:
    return [
        p for p in _raw_tokens(nome)
        if p not in STOPWORDS
        and p not in GENERIC_IDENTITY_TOKENS
        and len(p) > 1
    ]


def _tokens_fortes(nome: str) -> list:
    """Tokens de identidade com >= 4 chars."""
    return [p for p in _tokens_identidade(nome) if len(p) >= 4]


def _siglas_distintivas(nome: str) -> list:
    """Siglas 2-3 chars não genéricas. Palavras 4+ chars são tokens fortes."""
    return sorted({
        p for p in _raw_tokens(nome)
        if p not in STOPWORDS
        and p not in GENERIC_IDENTITY_TOKENS
        and 2 <= len(p) <= 3
    })


def _split_alnum(texto: str) -> list:
    return [p for p in re.split(r"[^a-z0-9]+", _norm(texto)) if p]


def _tem_sigla_exata(siglas: list, *textos: str) -> bool:
    """
    Sigla deve aparecer como token exato OU prefixo de token composto.
    'ldl' em 'ldlimoveis' = True. 'ldl' em 'mgfimoveis' = False.
    """
    if not siglas:
        return True
    tokens = set()
    for t in textos:
        tokens.update(_split_alnum(t))
    if any(s in tokens for s in siglas):
        return True
    for tok in tokens:
        for s in siglas:
            if len(tok) > len(s) + 2 and tok.startswith(s):
                return True
    return False


def _token_match_stats(nome: str, texto: str) -> tuple:
    texto_n = _norm(texto)
    hits = [p for p in _tokens_fortes(nome) if p in texto_n]
    uniq = sorted(set(hits), key=lambda x: (-len(x), x))
    return len(uniq), uniq


def _marca_alias_valida(nome: str, tokens_match: list) -> bool:
    if not tokens_match:
        return False
    return len(max(tokens_match, key=len)) >= 6


def _dominio_terceiro(url: str) -> bool:
    dom  = _dominio_limpo(url)
    path = urlparse(url).path.lower()
    q    = urlparse(url).query.lower()
    if any(d in dom  for d in DOMINIOS_TERCEIROS): return True
    if any(k in path for k in PATHS_TERCEIROS):    return True
    if "cnpj=" in q:                               return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# SELENIUM DRIVER
# ══════════════════════════════════════════════════════════════════════════════

def criar_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1280,900")
    opts.add_argument(f"user-agent={HEADERS['User-Agent']}")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


# ══════════════════════════════════════════════════════════════════════════════
# DUCKDUCKGO
# ══════════════════════════════════════════════════════════════════════════════

def _resolver_url_ddg(url: str):
    if not url:
        return None
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://duckduckgo.com" + url
    try:
        p = urlparse(url)
    except Exception:
        return None
    if "duckduckgo.com" in (p.netloc or "") and p.path.startswith("/l/"):
        qs   = parse_qs(p.query or "")
        uddg = qs.get("uddg", [None])[0]
        return unquote(uddg) if uddg else None
    if p.scheme in ("http", "https") and p.netloc:
        return url
    return None


def _buscar_duckduckgo(driver, query: str, n: int = 10) -> list:
    global _ddg_request_count

    if _ddg_request_count > 0:
        time.sleep(_DELAY_BASE + random.uniform(0, _DELAY_JITTER))

    url = f"https://duckduckgo.com/?q={quote(query)}&kl=br-pt"
    driver.get(url)
    _ddg_request_count += 1
    time.sleep(3.0 + random.uniform(0, 1.0))

    html = driver.page_source
    if len(html) < 5000 or "captcha" in html.lower():
        time.sleep(_DELAY_BLOQUEIO)
        return ["__BLOQUEADO__"]

    urls, vistos = [], set()

    for seletor in [
        "article h2 a",
        "[data-testid='result-title-a']",
        "h2.result__title a",
        ".result__a",
    ]:
        try:
            for el in driver.find_elements(_By.CSS_SELECTOR, seletor):
                href = el.get_attribute("href") or ""
                if "/l/?" in href or href.startswith("/"):
                    href = _resolver_url_ddg(href) or ""
                if not href or "duckduckgo.com" in href:
                    continue
                d = urlparse(href).netloc
                if d and d not in vistos:
                    vistos.add(d)
                    urls.append(href)
        except Exception:
            pass
        if urls:
            break

    if not urls:
        for bruto in re.findall(
                r'class="result__a"[^>]+href="([^"]+)"', html, re.IGNORECASE):
            final = _resolver_url_ddg(bruto)
            if not final:
                continue
            d = _dominio_limpo(final)
            if d and "duckduckgo.com" not in d and final not in vistos:
                vistos.add(final)
                urls.append(final)

    return urls[:n]


# ══════════════════════════════════════════════════════════════════════════════
# SITE
# ══════════════════════════════════════════════════════════════════════════════

def _pontuar_site_url(url: str, nome: str, cidade: str) -> int:
    dom  = _dominio_limpo(url)
    path = urlparse(url).path.lower()

    if "instagram.com" in dom:  return -1
    if _dominio_terceiro(url):  return  0
    for p in PORTAIS:
        if p in dom:            return  0

    exts = [".com.br", ".com", ".net.br", ".net", ".org.br", ".org", ".imb.br"]
    if not any(dom.endswith(e) for e in exts):
        return 0

    score = 0
    hits, tokens = _token_match_stats(nome, dom)
    score += hits * 4
    if tokens:
        score += 2

    siglas = _siglas_distintivas(nome)
    if siglas and _tem_sigla_exata(siglas, dom):
        score += 4

    if not _cidade_e_ambigua(cidade):
        if _norm(cidade).replace(" ", "")[:5] in dom:
            score += 1

    if dom.endswith(".com.br"): score += 1
    if path.count("/") > 3:     score -= 1
    return max(score, 0)


def _extrair_textos_site(html: str) -> dict:
    title = h1 = og_title = og_site_name = ""
    m = re.search(r"<title[^>]*>([^<]{2,180})</title>", html, re.IGNORECASE)
    if m: title = m.group(1).strip()
    m = re.search(r"<h1[^>]*>([^<]{2,180})</h1>", html, re.IGNORECASE)
    if m: h1 = m.group(1).strip()
    for pat in [
        r'property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m: og_title = m.group(1).strip(); break
    for pat in [
        r'property=["\']og:site_name["\'][^>]+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]+property=["\']og:site_name["\']',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m: og_site_name = m.group(1).strip(); break
    return {"title": title, "h1": h1, "og_title": og_title, "og_site_name": og_site_name}


def _extrair_ig_links(html: str) -> list:
    handles = []
    for m in re.finditer(r"instagram\.com/([a-zA-Z0-9_\.]{3,40})/?", html):
        h = m.group(1).lower()
        if h not in IGNORAR_IG and h not in handles:
            handles.append(h)
    return handles


def _buscar_pagina_auxiliar(site_url: str):
    base = _url_raiz(site_url)
    if not base:
        return None, None
    for c in ["contato", "fale-conosco", "sobre", "quem-somos", "empresa", "imoveis"]:
        try:
            r = requests.get(base + c, headers=HEADERS, timeout=TIMEOUT_PAGINA, allow_redirects=True)
            if r.status_code < 400 and "text/html" in r.headers.get("content-type", "").lower():
                return base + c, r.text
        except Exception:
            continue
    return None, None


def _verificar_site(url: str, nome: str, cidade: str) -> dict:
    vazio = {
        "ok": False, "score": 0, "motivo": "", "sinais": [],
        "conflitos": [], "ig_link": None, "url_final": None,
    }
    try:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA, allow_redirects=True)
            req_url = url
            if r.status_code >= 400:
                root = _url_raiz(url)
                if root and root != url:
                    r = requests.get(root, headers=HEADERS, timeout=TIMEOUT_RETRY, allow_redirects=True)
                    req_url = root
        except requests.Timeout:
            root = _url_raiz(url)
            if root and root != url:
                r = requests.get(root, headers=HEADERS, timeout=TIMEOUT_RETRY, allow_redirects=True)
                req_url = root
            else:
                return {**vazio, "motivo": "timeout"}

        if r.status_code >= 400:
            return {**vazio, "motivo": f"HTTP {r.status_code}", "url_final": r.url}

        final_url = r.url
        if _dominio_terceiro(final_url):
            return {**vazio, "motivo": "site_terceiro", "url_final": final_url}

        html      = r.text
        html_n    = _norm(html)
        text      = _extrair_textos_site(html)
        ig_links  = _extrair_ig_links(html)
        ig_link   = ig_links[0] if ig_links else None
        score     = 0
        sinais    = []
        conflitos = []

        brand_text = " ".join([
            _dominio_limpo(final_url),
            text["title"], text["h1"], text["og_title"], text["og_site_name"],
        ])
        hits_brand, tokens_brand = _token_match_stats(nome, brand_text)
        hits_body,  tokens_body  = _token_match_stats(nome, html_n)
        tokens_all = sorted(set(tokens_brand + tokens_body), key=lambda x: (-len(x), x))

        siglas   = _siglas_distintivas(nome)
        sigla_ok = _tem_sigla_exata(
            siglas,
            _dominio_limpo(final_url),
            text["title"], text["h1"], text["og_title"], text["og_site_name"],
        )

        if siglas and not sigla_ok:
            conflitos.append("sigla_distintiva_ausente")

        if hits_brand:
            score += hits_brand * 8; sinais.append(f"brand:{hits_brand}")
        if hits_body:
            score += min(hits_body * 4, 10); sinais.append(f"body:{hits_body}")
        if sigla_ok and siglas:
            score += 8; sinais.append("sigla_exata")

        hits_imob = sum(1 for t in TERMOS_IMOB if t in html_n)
        if hits_imob:
            score += min(hits_imob, 3); sinais.append(f"imob:{min(hits_imob, 3)}")

        cidade_ok = _norm(cidade) in html_n
        if cidade_ok:
            score += 8 if not _cidade_e_ambigua(cidade) else 3
            sinais.append("cidade_ok")

        # Página auxiliar: só quando há hit de marca mas falta cidade
        if hits_brand >= 1 and not cidade_ok and score < 26 and not conflitos:
            _, aux_html = _buscar_pagina_auxiliar(final_url)
            if aux_html:
                aux_n = _norm(aux_html)
                if _norm(cidade) in aux_n:
                    score += 6; cidade_ok = True; sinais.append("cidade_aux")
                aux_hits, _ = _token_match_stats(nome, aux_n)
                if aux_hits:
                    score += min(aux_hits * 3, 6); sinais.append(f"nome_aux:{aux_hits}")

        if ig_link:
            sinais.append(f"ig_site:@{ig_link}")
            ig_hits, _ = _token_match_stats(nome, ig_link)
            if ig_hits or (_marca_alias_valida(nome, tokens_all) and
                           any(tok in ig_link for tok in tokens_all)):
                score += 5

        ok = False; motivo = "site_não_confirmado"

        if _dominio_terceiro(final_url):
            ok = False; motivo = "site_terceiro"
        elif siglas and not sigla_ok:
            ok = False; motivo = "sigla_distintiva_ausente"
        elif hits_brand >= 1 and cidade_ok and score >= 18:
            ok = True;  motivo = "match_forte"
        elif hits_brand >= 2 and score >= 18:
            ok = True;  motivo = "match_composto"
        elif sigla_ok and siglas and score >= 18:
            ok = True;  motivo = "match_sigla_score"
        elif sigla_ok and siglas and (cidade_ok or hits_body >= 2) and score >= 14:
            ok = True;  motivo = "match_sigla_cidade"
        elif (_marca_alias_valida(nome, tokens_all) and cidade_ok and ig_link
              and any(tok in ig_link for tok in tokens_all)):
            ok = True; motivo = "alias_marca_comercial"; score += 6
            sinais.append("alias_confirmado")
        elif hits_brand >= 1 and cidade_ok and req_url != url:
            ok = True;  motivo = "match_revalidado_home"

        return {
            "ok": ok, "score": score, "motivo": motivo,
            "sinais": sinais, "conflitos": conflitos,
            "ig_link": ig_link, "url_final": _url_raiz(final_url) or final_url,
        }

    except requests.Timeout:
        return {**vazio, "motivo": "timeout"}
    except Exception as e:
        return {**vazio, "motivo": str(e)[:80]}


# ══════════════════════════════════════════════════════════════════════════════
# INSTAGRAM — fase 1 (identidade via requests)
# ══════════════════════════════════════════════════════════════════════════════

def _extrair_nome_display_ig(html: str):
    for pat in [
        r'property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            t = re.sub(r'\s*\(@[^)]+\).*', '', m.group(1)).strip()
            t = re.sub(r'\s*•.*', '', t).strip()
            if t: return t
    m = re.search(r'"full_name"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            import codecs
            return codecs.decode(m.group(1).replace('\\u', '\\u'), 'unicode_escape')
        except Exception:
            return m.group(1)
    return None


def _extrair_bio_ig(html: str) -> str:
    m = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.IGNORECASE)
    return m.group(1) if m else ""


def _extrair_link_bio_ig(html: str) -> str:
    for pat in [
        r'"external_url"\s*:\s*"([^"]{5,100})"',
        r'"biography_with_entities".*?"url"\s*:\s*"([^"]{5,100})"',
        r'<a[^>]+rel="nofollow"[^>]+href="([^"]{5,200})"',
    ]:
        m = re.search(pat, html)
        if m:
            u = m.group(1)
            if "instagram.com" not in u and len(u) > 5:
                return u
    return ""


def _pontuar_instagram_candidato(handle: str, nome: str, cidade: str, origem: str) -> int:
    if handle in IGNORAR_IG or len(handle) < 3:
        return 0
    score = 0
    hits, _ = _token_match_stats(nome, handle)
    if hits: score += hits * 4
    siglas = _siglas_distintivas(nome)
    if siglas and _tem_sigla_exata(siglas, handle): score += 8
    if any(t in handle for t in ["imob", "imovel", "imoveis", "corretor"]): score += 2
    cn = _norm(cidade).replace(" ", "")
    if cn[:4] in _norm(handle) or cn[:5] in _norm(handle): score += 1
    bonus = {"site_validado": 8, "site_html": 6, "resultado_site": 4, "ddg_ig": 0}
    score += bonus.get(origem, 0)
    return max(score, 0)


def _verificar_identidade_ig(handle: str, nome: str, cidade: str, site_url=None) -> dict:
    """
    Fase 1: identidade via requests (sem Selenium).
    8 caminhos de aprovação + proteção de ambiguidade por token principal.
    """
    url = f"https://www.instagram.com/{handle}/"
    neg = {
        "confirmado": False, "score": 0, "reciproco": False,
        "link_bio": "", "handle_hits": 0, "display_hits": 0,
        "bio_hits": 0, "sigla_ok": False, "motivo": "",
    }
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA)
        if r.status_code == 404:
            return {**neg, "motivo": "perfil_nao_existe"}
        if r.status_code != 200:
            return {**neg, "motivo": f"HTTP {r.status_code}"}

        html = r.text
        if "this account is private" in html.lower():
            return {**neg, "motivo": "privado"}

        display_name = _extrair_nome_display_ig(html) or ""
        bio          = _extrair_bio_ig(html)
        link_bio     = _extrair_link_bio_ig(html)

        handle_hits,  ht = _token_match_stats(nome, handle)
        display_hits, dt = _token_match_stats(nome, display_name)
        bio_hits,     bt = _token_match_stats(nome, bio)
        siglas   = _siglas_distintivas(nome)
        sigla_ok = _tem_sigla_exata(siglas, handle, display_name, bio)
        tokens_all = sorted(set(ht + dt + bt), key=lambda x: (-len(x), x))

        score  = 0
        sinais = []
        if handle_hits:  score += handle_hits  * 8; sinais.append(f"handle:{handle_hits}")
        if display_hits: score += display_hits * 7; sinais.append(f"display:{display_hits}")
        if bio_hits:     score += min(bio_hits * 4, 8); sinais.append(f"bio:{bio_hits}")
        if sigla_ok and siglas: score += 8; sinais.append("sigla_exata")
        if _norm(cidade) in _norm(bio): score += 4; sinais.append("cidade_bio")

        reciproco = False
        if site_url:
            dom_site = _dominio_limpo(site_url)
            if dom_site and dom_site in html.lower():
                reciproco = True; score += 5; sinais.append("reciproco_site_no_html")
            if link_bio and dom_site and _dominio_limpo(link_bio) == dom_site:
                reciproco = True; score += 8; sinais.append("link_bio_igual_site")
            elif link_bio and dom_site:
                bdh, _ = _token_match_stats(nome, _dominio_limpo(link_bio))
                if bdh: score += 4; sinais.append("link_bio_coerente_dominio")

        # ── Proteção de ambiguidade ───────────────────────────────────────
        todos_perfil    = " ".join([handle, display_name, bio])
        tokens_f_nome   = _tokens_fortes(nome)
        excl_ausentes   = [t for t in tokens_f_nome if len(t) >= 5 and t not in _norm(todos_perfil)]
        if excl_ausentes:
            score -= len(excl_ausentes) * 6
            sinais.append(f"tokens_exclusivos_ausentes:{excl_ausentes}")
            token_principal = max(tokens_f_nome, key=len) if tokens_f_nome else ""
            if (token_principal and len(token_principal) >= 5
                    and token_principal not in _norm(todos_perfil)
                    and len(tokens_f_nome) >= 2):
                sinais.append("ambiguidade_bloqueio")

        # Pré-bloqueio
        if "ambiguidade_bloqueio" in sinais:
            return {
                **neg,
                "score": score, "reciproco": reciproco, "link_bio": link_bio,
                "handle_hits": handle_hits, "display_hits": display_hits,
                "bio_hits": bio_hits, "sigla_ok": sigla_ok,
                "display_name": display_name,
                "motivo": f"score={score} [{', '.join(sinais)}]",
                "html": html,
            }

        # ── 8 caminhos de confirmação ─────────────────────────────────────
        confirmado    = False
        tem_tf        = bool(tokens_f_nome)
        cidade_na_bio = _norm(cidade) in _norm(bio)

        if siglas and not sigla_ok and not tem_tf:
            sinais.append("sigla_ausente_bloqueio")
        elif ((handle_hits > 0 and display_hits > 0)
              or (handle_hits > 0 and bio_hits > 0)
              or (display_hits > 0 and bio_hits > 0)):
            confirmado = True
        elif sigla_ok and siglas and (
            handle_hits > 0 or display_hits > 0
            or _tem_sigla_exata(siglas, handle)
            or _tem_sigla_exata(siglas, display_name)
        ):
            confirmado = True
        elif sigla_ok and siglas and (cidade_na_bio or reciproco):
            confirmado = True; sinais.append("sigla_cidade_bio")
        elif handle_hits > 0 and reciproco:
            confirmado = True; sinais.append("handle_reciproco")
        elif handle_hits > 0 and cidade_na_bio:
            confirmado = True; sinais.append("handle_cidade_bio")
        elif display_hits > 0 and cidade_na_bio:
            confirmado = True; sinais.append("display_cidade_bio")
        elif _marca_alias_valida(nome, tokens_all):
            alias = max(tokens_all, key=len) if tokens_all else ""
            if alias and (alias in _norm(handle) or alias in _norm(display_name)):
                if bio_hits > 0 or reciproco or cidade_na_bio:
                    confirmado = True; sinais.append("alias_confirmado")

        return {
            "confirmado": confirmado, "score": score, "reciproco": reciproco,
            "link_bio": link_bio, "handle_hits": handle_hits,
            "display_hits": display_hits, "bio_hits": bio_hits,
            "sigla_ok": sigla_ok, "display_name": display_name,
            "motivo": f"score={score} [{', '.join(sinais) or 'sem_sinais'}]",
            "html": html,
        }
    except Exception as e:
        return {**neg, "motivo": str(e)[:80]}


# ══════════════════════════════════════════════════════════════════════════════
# INSTAGRAM — fase 2 (métricas via Selenium)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_numero(s: str):
    try:
        s       = s.strip()
        tem_mil = "mil" in s.lower()
        tem_k   = s.lower().endswith("k") and not tem_mil
        s_num   = re.sub(r"(?:mil|k)", "", s, flags=re.IGNORECASE).strip()
        if tem_mil or tem_k:
            s_num = s_num.replace(".", "").replace(",", ".")
            try:    valor = float(s_num)
            except Exception:
                m = re.search(r"\d+", s_num); valor = float(m.group()) if m else 0.0
            return int(round(valor * 1000))
        return int(s_num.replace(".", "").replace(",", ""))
    except Exception:
        return None


def _extrair_datas_json(html: str) -> list:
    """Extrai datas de posts do JSON embutido (taken_at_timestamp, ISO 8601)."""
    datas = set()
    for pat in [
        r'"taken_at_timestamp"\s*:\s*(\d{10})',
        r'"taken_at"\s*:\s*(\d{10})',
        r'"timestamp"\s*:\s*(\d{10})',
        r'"created_time"\s*:\s*"(\d{10})"',
    ]:
        for ts in re.findall(pat, html):
            try:
                d = _dt.fromtimestamp(int(ts), tz=_tz.utc).strftime("%Y-%m-%d")
                if d > "2015-01-01": datas.add(d)
            except Exception:
                pass
    for iso in re.findall(r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^"]{0,10})"', html):
        try:
            d = iso[:10]
            if d > "2015-01-01": datas.add(d)
        except Exception:
            pass
    for d in re.findall(r'"date"\s*:\s*"(\d{4}-\d{2}-\d{2})"', html):
        if d > "2015-01-01": datas.add(d)
    return sorted(datas, reverse=True)[:20]


def _extrair_data_abrindo_posts(driver, html_perfil: str):
    """Abre posts individuais para extrair data (mais confiável que DOM do perfil)."""
    try:
        time.sleep(1.5)
        hrefs, vistos = [], set()
        for sel in ["article a[href*='/p/']", "a[href*='/p/']", "main article a"]:
            try:
                for p in driver.find_elements(_By.CSS_SELECTOR, sel):
                    href = p.get_attribute("href") or ""
                    if "/p/" in href and href not in vistos:
                        vistos.add(href); hrefs.append(href)
                    if len(hrefs) >= 4: break
                if hrefs: break
            except Exception:
                pass

        if not hrefs: return None
        url_perfil = driver.current_url
        datas = []

        for idx, href in enumerate(hrefs[:4]):
            try:
                driver.get(href)
                time.sleep(2.5)
                try:
                    tel = _WDW(driver, 5).until(
                        _EC.presence_of_element_located((_By.CSS_SELECTOR, "time[datetime]")))
                    dt_val = tel.get_attribute("datetime") or ""
                    if dt_val: datas.append(dt_val[:10]); continue
                except Exception:
                    pass
                ph = driver.page_source
                ds = _extrair_datas_json(ph)
                if ds: datas.append(ds[0]); continue
                m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', ph)
                if m: datas.append(m.group(1))
            except Exception:
                continue

        try:
            driver.get(url_perfil); time.sleep(1.5)
        except Exception:
            pass

        if not datas: return None
        return sorted(datas, reverse=True)[0]
    except Exception:
        return None


def _verificar_metricas_ig(driver, handle: str, html_requests: str = None) -> dict:
    """Fase 2: Selenium para seguidores, posts e data do último post."""
    res = {"seguidores": None, "num_posts": None, "ultimo_post": None, "motivo": None}

    # Pré-extração do HTML da fase 1
    if html_requests:
        meta_m = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            html_requests, re.IGNORECASE)
        if meta_m:
            desc = meta_m.group(1)
            sm = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)', desc, re.IGNORECASE)
            pm = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)', desc, re.IGNORECASE)
            if sm: res["seguidores"] = _parse_numero(sm.group(1))
            if pm: res["num_posts"]  = _parse_numero(pm.group(1))
        datas_req = _extrair_datas_json(html_requests)
        if datas_req: res["ultimo_post"] = datas_req[0]

    url = f"https://www.instagram.com/{handle}/"
    try:
        driver.get(url)
        time.sleep(3.0)
        driver.execute_script("window.scrollTo(0, 600)")
        time.sleep(0.8)
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(0.5)

        html_sel = driver.page_source
        if "sorry, this page" in html_sel.lower() or "página não disponível" in html_sel.lower():
            res["motivo"] = "perfil_nao_existe"; return res

        # Seguidores/posts M1: meta description
        for src in [html_sel] + ([html_requests] if html_requests else []):
            if res["seguidores"] is not None and res["num_posts"] is not None: break
            mm = re.search(
                r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
                src, re.IGNORECASE)
            if mm:
                desc = mm.group(1)
                if res["seguidores"] is None:
                    sm = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)', desc, re.IGNORECASE)
                    if sm: res["seguidores"] = _parse_numero(sm.group(1))
                if res["num_posts"] is None:
                    pm = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)', desc, re.IGNORECASE)
                    if pm: res["num_posts"] = _parse_numero(pm.group(1))

        # Seguidores/posts M2: JSON embutido
        if res["seguidores"] is None:
            for pat in [
                r'"edge_followed_by":\{"count":(\d+)\}',
                r'"follower_count":(\d+)', r'"followers_count":(\d+)',
                r'"followers":(\d+)', r'"userInteractionCount":(\d+)',
            ]:
                m = re.search(pat, html_sel)
                if m: res["seguidores"] = int(m.group(1)); break

        if res["num_posts"] is None:
            for pat in [r'"edge_owner_to_timeline_media":\{"count":(\d+)', r'"media_count":(\d+)']:
                m = re.search(pat, html_sel)
                if m: res["num_posts"] = int(m.group(1)); break

        # Seguidores/posts M3: elementos li com contexto
        if res["seguidores"] is None or res["num_posts"] is None:
            try:
                for sel_css in ["header section ul li", "section ul li"]:
                    try:
                        for li in driver.find_elements(_By.CSS_SELECTOR, sel_css)[:6]:
                            txt = (li.text or "").lower().strip()
                            if not txt: continue
                            mn = re.search(r'([\d\.\,]+(?:\s*(?:mil|k))?)', txt)
                            if not mn: continue
                            num = _parse_numero(mn.group(1))
                            if num is None: continue
                            if any(p in txt for p in ["publicaç", "post"]):
                                if res["num_posts"] is None: res["num_posts"] = num
                            elif any(p in txt for p in ["seguidor", "follower"]):
                                if res["seguidores"] is None: res["seguidores"] = num
                        if res["seguidores"] is not None: break
                    except Exception:
                        continue
            except Exception:
                pass

        # Seguidores/posts M4: spans com contexto JS
        if res["seguidores"] is None or res["num_posts"] is None:
            try:
                spans = driver.find_elements(
                    _By.CSS_SELECTOR,
                    "header section ul li span span, "
                    "header section ul li span[class*='x5n08af']")
                for span in spans[:12]:
                    txt = (span.text or "").strip()
                    if not re.search(r'^\d', txt.replace(".", "").replace(",", "")): continue
                    try:
                        li_pai = driver.execute_script("return arguments[0].closest('li')", span)
                        ctx = (li_pai.text or "").lower() if li_pai else ""
                    except Exception:
                        ctx = ""
                    num = _parse_numero(txt)
                    if num is None: continue
                    if any(p in ctx for p in ["publicaç", "post"]):
                        if res["num_posts"] is None: res["num_posts"] = num
                    elif any(p in ctx for p in ["seguidor", "follower"]):
                        if res["seguidores"] is None: res["seguidores"] = num
            except Exception:
                pass

        # Data D1: JSON do Selenium
        datas_sel = _extrair_datas_json(html_sel)
        if datas_sel and (res["ultimo_post"] is None or datas_sel[0] > res["ultimo_post"]):
            res["ultimo_post"] = datas_sel[0]

        # Data D2: posts individuais (mais confiável)
        if res["ultimo_post"] is None:
            res["ultimo_post"] = _extrair_data_abrindo_posts(driver, html_sel)

        # Data D3: scroll + time[datetime] no DOM
        if res["ultimo_post"] is None:
            try:
                driver.execute_script("window.scrollTo(0, 600)")
                time.sleep(0.8)
                driver.execute_script("window.scrollTo(0, 0)")
                time.sleep(0.5)
                html_scroll = driver.page_source
                datas_scroll = _extrair_datas_json(html_scroll)
                if datas_scroll:
                    res["ultimo_post"] = datas_scroll[0]
                else:
                    datas_dom = []
                    for sel_t in ["time[datetime]", "article time", "main time"]:
                        try:
                            for tel in driver.find_elements(_By.CSS_SELECTOR, sel_t)[:15]:
                                d = (tel.get_attribute("datetime") or "")[:10]
                                if d > "2015-01-01": datas_dom.append(d)
                        except Exception:
                            continue
                    if datas_dom:
                        res["ultimo_post"] = sorted(datas_dom, reverse=True)[0]
            except Exception:
                pass

        # Data D4: JavaScript direto
        if res["ultimo_post"] is None:
            try:
                datas_js = driver.execute_script("""
                    var datas = [];
                    document.querySelectorAll('time[datetime], article time')
                        .forEach(function(el) {
                            var d = el.getAttribute('datetime') || '';
                            if (d.length >= 10) datas.push(d.substring(0, 10));
                        });
                    document.querySelectorAll('a[href*="/p/"]').forEach(function(a) {
                        var t = a.querySelector('time');
                        if (t) {
                            var d = t.getAttribute('datetime') || '';
                            if (d.length >= 10) datas.push(d.substring(0, 10));
                        }
                    });
                    return datas;
                """) or []
                validas = sorted([d for d in datas_js if d > "2015-01-01"], reverse=True)
                if validas: res["ultimo_post"] = validas[0]
            except Exception:
                pass

    except Exception as e:
        if not res.get("motivo"):
            res["motivo"] = f"selenium: {str(e)[:60]}"

    return res


def _extrair_ig_do_site(site_url: str, nome: str, cidade: str):
    try:
        r = requests.get(site_url, headers=HEADERS, timeout=TIMEOUT_PAGINA)
        if r.status_code != 200: return None
        handles = _extrair_ig_links(r.text)
        if handles:
            return max(handles, key=lambda h: _pontuar_instagram_candidato(h, nome, cidade, "site_html"))
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DESCOBERTA DE SITE
# ══════════════════════════════════════════════════════════════════════════════

def _descobrir_site(driver, nome: str, cidade: str, log) -> dict:
    queries = [
        f'"{nome}" {_cidade_query(cidade)} imobiliária',
        f'{nome} {_cidade_query(cidade)} imóveis site',
    ]
    handles_ig = []
    candidatos = []
    vistos     = set()

    for i, query in enumerate(queries, 1):
        log(f"  🌐 DDG ({i}/{len(queries)}): {query}")
        urls = _buscar_duckduckgo(driver, query, n=8 if i == 1 else 6)
        if urls == ["__BLOQUEADO__"]:
            log("    ⚠️ DDG bloqueado"); continue

        for url in urls:
            score = _pontuar_site_url(url, nome, cidade)
            if score == -1:
                m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,40})/?', url)
                if m:
                    h = m.group(1).lower()
                    if h not in IGNORAR_IG and h not in handles_ig:
                        handles_ig.append(h)
                continue
            if score > 0 and url not in vistos:
                vistos.add(url); candidatos.append({"url": url, "score": score})

    candidatos = sorted(candidatos, key=lambda x: -x["score"])

    if not candidatos:
        log("    → Nenhum candidato de site elegível")
        return {
            "url": None, "confirmado": False, "sem_site": True,
            "ig_dos_resultados": handles_ig, "ig_do_site_validado": None,
            "site_score": 0, "site_sinais": [], "site_motivo": "sem_candidatos",
        }

    for cand in candidatos[:MAX_TENTATIVAS_SITE]:
        url = cand["url"]
        log(f"    → verificando: {url[:60]} (score_url={cand['score']})")
        v = _verificar_site(url, nome, cidade)
        conflito_txt = f" | conflitos={v['conflitos']}" if v.get("conflitos") else ""
        log(f"    {'✅' if v['ok'] else '❌'} {v['motivo']} | sinais={v.get('sinais',[])} score={v.get('score',0)}{conflito_txt}")

        if v["ok"]:
            if v.get("ig_link"):
                log(f"    🔗 IG no site: @{v['ig_link']}")
            return {
                "url": v["url_final"], "confirmado": True, "sem_site": False,
                "ig_dos_resultados": handles_ig,
                "ig_do_site_validado": v.get("ig_link"),
                "site_score": v["score"], "site_sinais": v["sinais"],
                "site_motivo": v["motivo"],
            }

    return {
        "url": None, "confirmado": False, "sem_site": True,
        "ig_dos_resultados": handles_ig, "ig_do_site_validado": None,
        "site_score": 0, "site_sinais": [], "site_motivo": "site_nao_confirmado",
    }


# ══════════════════════════════════════════════════════════════════════════════
# DESCOBERTA DE INSTAGRAM
# ══════════════════════════════════════════════════════════════════════════════

def _descobrir_instagram(driver, nome: str, cidade: str, site_url: str,
                          ig_do_site_validado: str, ig_dos_resultados: list, log) -> dict:
    log(f"  📸 Buscando Instagram: {nome}")
    candidatos = []
    vistos     = set()

    if ig_do_site_validado and ig_do_site_validado not in vistos:
        vistos.add(ig_do_site_validado)
        s = _pontuar_instagram_candidato(ig_do_site_validado, nome, cidade, "site_validado")
        candidatos.append({"handle": ig_do_site_validado, "score": s, "origem": "site_validado"})
        log(f"    🥇 Site validado: @{ig_do_site_validado} (score={s})")

    if site_url:
        h = _extrair_ig_do_site(site_url, nome, cidade)
        if h and h not in vistos:
            vistos.add(h); s = _pontuar_instagram_candidato(h, nome, cidade, "site_html")
            candidatos.append({"handle": h, "score": s, "origem": "site_html"})
            log(f"    🥈 HTML do site: @{h} (score={s})")

    for h in ig_dos_resultados:
        if h not in vistos:
            vistos.add(h); s = _pontuar_instagram_candidato(h, nome, cidade, "resultado_site")
            candidatos.append({"handle": h, "score": s, "origem": "resultado_site"})

    if len(candidatos) < 3:
        query = f'{nome} {_cidade_query(cidade)} site:instagram.com'
        urls  = _buscar_duckduckgo(driver, query, n=10)
        if urls != ["__BLOQUEADO__"]:
            for url in urls:
                m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,40})/?', url)
                if not m: continue
                h = m.group(1).lower()
                if h in IGNORAR_IG or h in vistos: continue
                vistos.add(h); s = _pontuar_instagram_candidato(h, nome, cidade, "ddg_ig")
                candidatos.append({"handle": h, "score": s, "origem": "ddg_ig"})
                log(f"    🔍 DDG: @{h} (score={s})")

    candidatos = sorted(
        [c for c in candidatos if c["score"] > 0 or c["origem"] in ("site_validado", "site_html")],
        key=lambda x: -x["score"])

    if not candidatos:
        log("    → Nenhum candidato de Instagram")
        return {"url": None, "handle": None, "confirmado": False, "sem_ig": True,
                "metodo": None, "origem": None, "motivo": "sem_candidatos"}

    for cand in candidatos[:MAX_TENTATIVAS_IG]:
        h      = cand["handle"]
        origem = cand["origem"]
        log(f"    → verificando @{h} (score={cand['score']}, origem={origem})")

        ident    = _verificar_identidade_ig(h, nome, cidade, site_url=site_url)
        aprovado = ident["confirmado"]

        if not aprovado and origem in ("site_validado", "site_html"):
            tem_hit = (ident.get("handle_hits", 0) > 0
                       or ident.get("display_hits", 0) > 0
                       or ident.get("bio_hits", 0) > 0)
            sigla_bloqueia = (_siglas_distintivas(nome)
                              and not ident.get("sigla_ok", False)
                              and not bool(_tokens_fortes(nome)))
            if tem_hit and not sigla_bloqueia:
                aprovado = True; log(f"    ↗️ Aprovado por origem confiável ({origem})")

        if not aprovado and origem == "resultado_site" and ident.get("reciproco"):
            if ident.get("handle_hits", 0) > 0 or ident.get("display_hits", 0) > 0:
                aprovado = True; log(f"    ↗️ Aprovado: resultado_site recíproco")

        if not aprovado and origem == "ddg_ig" and ident.get("reciproco"):
            if ident.get("handle_hits", 0) > 0 or ident.get("display_hits", 0) > 0:
                aprovado = True; log(f"    ↗️ ddg_ig aprovado: recíproco + hit")

        if aprovado and origem == "ddg_ig" and not ident.get("reciproco"):
            motivo_str = ident.get("motivo", "")
            tem_forte = any(s in motivo_str for s in
                            ["handle_cidade_bio", "display_cidade_bio", "sigla_cidade_bio"])
            if not tem_forte and not (ident.get("handle_hits", 0) > 0
                                      and ident.get("display_hits", 0) > 0):
                log(f"    ⛔ ddg_ig sem evidência suficiente — rejeitado")
                aprovado = False

        if not aprovado:
            log(f"    ❌ identidade: {ident['motivo']}"); continue

        log(f"    ✅ identidade: {ident['motivo']}")

        metricas = _verificar_metricas_ig(driver, h, html_requests=ident.get("html"))
        seg   = metricas.get("seguidores")
        posts = metricas.get("num_posts")
        data  = metricas.get("ultimo_post")
        log(f"    📊 seg={seg} posts={posts} último={data}")

        if seg is not None and seg < MIN_SEGUIDORES:
            log(f"    ❌ poucos seguidores ({seg})"); continue
        if posts is not None and posts < MIN_POSTS:
            log(f"    ❌ poucos posts ({posts})"); continue

        semanas = None
        if data:
            try:
                semanas = (date.today() - date.fromisoformat(data)).days // 7
                if semanas > MAX_SEMANAS_INATIVO:
                    log(f"    ❌ inativo: {semanas} semanas"); continue
            except Exception:
                pass
        else:
            if origem in ("site_validado", "site_html"):
                log(f"    ⚠️ data não detectada (origem={origem}) — flag revisão")
            elif origem == "resultado_site" and ident.get("reciproco"):
                log(f"    ⚠️ data não detectada (recíproco) — flag revisão")
            else:
                log(f"    ❌ data não detectada (origem={origem}) — rejeitado"); continue

        motivo_final = ident["motivo"]
        if metricas.get("motivo"):
            motivo_final += f" | {metricas['motivo']}"

        return {
            "url": f"https://www.instagram.com/{h}/",
            "handle": h, "confirmado": True, "sem_ig": False,
            "metodo": origem, "origem": origem,
            "reciproco": ident.get("reciproco", False),
            "seguidores": seg, "num_posts": posts,
            "ultimo_post": data, "semanas": semanas,
            "motivo": motivo_final,
        }

    return {"url": None, "handle": None, "confirmado": False, "sem_ig": True,
            "metodo": None, "origem": None, "motivo": "nao_confirmado"}


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR
# ══════════════════════════════════════════════════════════════════════════════

def enriquecer(driver, nome: str, cidade: str, log=None) -> ResultadoEnriquecimento:
    """Ponto de entrada. Descobre site + Instagram e retorna ResultadoEnriquecimento."""
    def _log(msg):
        if log: log(msg)

    r = ResultadoEnriquecimento(nome=nome, cidade=cidade)
    try:
        site = _descobrir_site(driver, nome, cidade, _log)
        r.site_url        = site["url"]
        r.site_confirmado = site["confirmado"]
        r.site_score      = site.get("site_score", 0)
        r.site_sinais     = site.get("site_sinais", [])
        r.site_motivo     = site.get("site_motivo")
        r.sem_site        = site["sem_site"]

        if r.sem_site or not r.site_confirmado:
            r.sem_instagram      = True
            r.ig_motivo          = "instagram_pulado_sem_site"
            r.motivo_rejeicao_ig = r.ig_motivo
            _log("  ⏭️ Instagram pulado — site não confirmado")
            return r

        time.sleep(1)

        ig = _descobrir_instagram(
            driver, nome, cidade,
            site_url=r.site_url,
            ig_do_site_validado=site.get("ig_do_site_validado"),
            ig_dos_resultados=site.get("ig_dos_resultados", []),
            log=_log,
        )
        r.instagram_url      = ig.get("url")
        r.ig_handle          = ig.get("handle")
        r.ig_confirmado      = ig.get("confirmado", False)
        r.ig_metodo          = ig.get("metodo")
        r.ig_origem          = ig.get("origem")
        r.ig_reciproco       = ig.get("reciproco", False)
        r.ig_seguidores      = ig.get("seguidores")
        r.ig_num_posts       = ig.get("num_posts")
        r.ig_ultimo_post     = ig.get("ultimo_post")
        r.ig_semanas         = ig.get("semanas")
        r.sem_instagram      = ig.get("sem_ig", True)
        r.ig_motivo          = ig.get("motivo")
        r.motivo_rejeicao_ig = ig.get("motivo")

        flags = []
        if r.site_url and r.site_score < 25:
            flags.append(f"site:score_baixo:{r.site_score}")
        if r.instagram_url and not r.ig_ultimo_post:
            flags.append("ig:data_nao_detectada")
        if r.instagram_url and r.ig_seguidores is None:
            flags.append("ig:seguidores_nao_verificados")
        if r.instagram_url and r.ig_origem == "ddg_ig" and not r.ig_reciproco:
            flags.append("ig:sem_reciprocidade")
        r.review_flags = flags

        if r.sem_instagram:
            _log("  ⏭️ Anúncios pulados — Instagram não confirmado")

    except Exception as e:
        r.erro = str(e)
        _log(f"  ⚠️ Erro inesperado: {e}")

    return r
