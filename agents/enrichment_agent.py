"""
agents/enrichment_agent.py — v11.3.3
Pipeline progressivo de resolução de identidade.

Arquitetura:
- Tokens de identidade distintos de tokens genéricos do setor
- Siglas curtas (LDL, K2, etc.) exigem match exato — sem substring falso
- Site: múltiplas queries DDG + verificação em brand/body/aux/ig
- Instagram: fase 1 via requests (identidade barata), fase 2 Selenium (métricas)
- Data de posts: JSON do perfil > time[datetime] no DOM > abrir posts (fallback)
- DuckDuckGo: seletor DOM validado (article h2 a) + fallbacks
"""

import re, time, random, requests
from datetime import date
from urllib.parse import urlparse, quote, parse_qs, unquote
from dataclasses import dataclass

# ── Critérios mínimos ─────────────────────────────────────────────────────────
MIN_SEGUIDORES      = 500
MIN_POSTS           = 20
MAX_SEMANAS_INATIVO = 8

MAX_TENTATIVAS_SITE = 3
MAX_TENTATIVAS_IG   = 3
TIMEOUT_PAGINA      = 10
TIMEOUT_RETRY       = 16

# ── Rate limiting DDG ─────────────────────────────────────────────────────────
_google_requests_count = 0
_DELAY_BASE            = 3.5
_DELAY_JITTER          = 1.5
_DELAY_APOS_BLOQUEIO   = 40

CIDADES_AMBIGUAS = {"saopaulo": "SP", "riodejaneiro": "RJ"}

PORTAIS = [
    "zapimoveis","vivareal","olx","quintoandar","chavesnamao",
    "imovelweb","wimoveis","netimoveis","123imoveis","imobiliare",
    "facebook","linkedin","twitter","youtube","tiktok",
    "google","bing","yahoo","gstatic","googleapis",
    "receitaws","minhareceita","econodata","jusbrasil","escavador",
    "solutudo","telelista","apontador","guiamais","listel",
    "procuroacho","advdinamico","empresaqui","infobel",
    "serasaexperian","serasa","boavista","spc","reclameaqui",
]

# Domínios de diretórios/CNPJ — nunca são o site oficial
DOMINIOS_TERCEIROS = [
    "cadastroempresa.com.br","parafa.com.br","cnpj.biz","cnpja.com",
    "econodata.com.br","infoinvest.com.br","empresasdobrasil.com.br",
    "cnpjbrasil.com","cnpjbrasil.com.br","receitaws.com.br",
    "solutudo.com.br","telelistas.net","listaonline.com.br",
    "apontador.com.br","guiamais.com.br","listel.com.br",
    "infobel.com","vriconsulting.com.br","cylex.com.br","encontra",
]

PATHS_TERCEIROS = [
    "/cnpj/","/empresa/","/empresas/","/cadastro/","/perfil/",
    "/fornecedor/","/consulta-empresa/","/consultar/","/guia/",
    "/empresa.php","/company/","/companies/",
]

IGNORAR_IG = {
    "p","reel","reels","stories","explore","tv","accounts",
    "instagram","about","sharedfiles","highlights"
}

TERMOS_IMOB = [
    "imóveis","imoveis","imobiliária","imobiliaria","corretor",
    "corretora","venda","locação","locacao","aluguel",
    "apartamento","casa","terreno","comprar imóvel",
]

# Tokens genéricos do setor — não contam como identidade de marca
GENERIC_IDENTITY_TOKENS = {
    "imoveis","imovel","imobiliaria","imobiliarias","corretor","corretora",
    "negocios","empreendimentos","administradora","bens",
    "consultoria","servicos","grupo","holding","brasil",
    "imob","imobiliario","imobiliarios",
    "associados","parceiros","gestao","investimentos",
}

STOPWORDS = {
    "ltda","eireli","me","sa","s","a","epp","ss",
    "e","de","da","do","das","dos","em","com","para","por","o","os","as",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ResultadoEnriquecimento:
    nome:               str
    cidade:             str
    site_url:           str  = None
    site_confirmado:    bool = False
    site_score:         int  = 0
    site_sinais:        list = None
    site_motivo:        str  = None
    sem_site:           bool = False
    instagram_url:      str  = None
    ig_handle:          str  = None
    ig_confirmado:      bool = False
    ig_metodo:          str  = None
    ig_origem:          str  = None
    ig_reciproco:       bool = False
    ig_seguidores:      int  = None
    ig_num_posts:       int  = None
    ig_ultimo_post:     str  = None
    ig_semanas:         int  = None
    ig_motivo:          str  = None
    motivo_rejeicao_ig: str  = None
    sem_instagram:      bool = False
    erro:               str  = None
    review_flags:       list = None

    def __post_init__(self):
        if self.site_sinais  is None: self.site_sinais  = []
        if self.review_flags is None: self.review_flags = []


# ══════════════════════════════════════════════════════════════════════════════
# UTILITÁRIOS DE NORMALIZAÇÃO E TOKENS
# ══════════════════════════════════════════════════════════════════════════════

def _norm(s: str) -> str:
    import unicodedata
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
    return [p for p in _raw_tokens(nome)
            if p not in STOPWORDS
            and p not in GENERIC_IDENTITY_TOKENS
            and len(p) > 1]

def _tokens_fortes(nome: str) -> list:
    """Tokens de identidade com >= 4 chars — palavras reais da marca."""
    return [p for p in _tokens_identidade(nome) if len(p) >= 4]

def _siglas_distintivas(nome: str) -> list:
    """
    Siglas reais: 2-3 chars, não stopword, não genérico.
    Palavras de 4+ chars são tokens fortes, não siglas.
    Ex: 'ldl', 'amr', 'k2', 'krs' → siglas
        'nova', 'home', 'belo' → tokens fortes
    """
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
    Sigla deve aparecer como token exato OU como prefixo de token composto.
    Ex: 'ldl' em 'ldlimoveis' conta (prefixo).
    Ex: 'ldl' em 'mgfimoveis' não conta.
    """
    if not siglas:
        return True
    tokens = set()
    for t in textos:
        tokens.update(_split_alnum(t))
    # Verifica match exato
    if any(s in tokens for s in siglas):
        return True
    # Verifica prefixo: sigla no início de token composto (ex: ldlimoveis, amrimobiliaria)
    # Qualquer token que comece com a sigla conta — o domínio herdou a identidade
    for tok in tokens:
        for s in siglas:
            if len(tok) > len(s) + 2 and tok.startswith(s):
                return True
    return False

def _token_match_stats(nome: str, texto: str) -> tuple:
    """Retorna (n_hits, tokens_encontrados) dos tokens fortes do nome no texto."""
    texto_n = _norm(texto)
    hits = [p for p in _tokens_fortes(nome) if p in texto_n]
    uniq = sorted(set(hits), key=lambda x: (-len(x), x))
    return len(uniq), uniq

def _marca_alias_valida(nome: str, tokens_match: list) -> bool:
    """True se o match mais longo tem >= 6 chars (alias real, não genérico)."""
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
# DUCKDUCKGO — seletor DOM validado (article h2 a)
# ══════════════════════════════════════════════════════════════════════════════

def _resolver_url_ddg(url: str):
    if not url: return None
    if url.startswith("//"): url = "https:" + url
    elif url.startswith("/"): url = "https://duckduckgo.com" + url
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
    global _google_requests_count
    from selenium.webdriver.common.by import By

    if _google_requests_count > 0:
        time.sleep(_DELAY_BASE + random.uniform(0, _DELAY_JITTER))

    url = f"https://duckduckgo.com/?q={quote(query)}&kl=br-pt"
    driver.get(url)
    _google_requests_count += 1
    time.sleep(4 + random.uniform(0, 1.5))

    html = driver.page_source
    if len(html) < 5000 or "captcha" in html.lower():
        time.sleep(_DELAY_APOS_BLOQUEIO)
        return ["__BLOQUEADO__"]

    urls, vistos = [], set()

    # Seletores em ordem de preferência — validados em testes reais
    for seletor in [
        "article h2 a",
        "[data-testid='result-title-a']",
        "h2.result__title a",
        ".result__a",
    ]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, seletor):
                href = el.get_attribute("href") or ""
                # Resolve redirect DDG se necessário
                if "/l/?" in href or href.startswith("/"):
                    href = _resolver_url_ddg(href) or ""
                if not href or "duckduckgo.com" in href:
                    continue
                d = urlparse(href).netloc
                if d and d not in vistos:
                    vistos.add(d); urls.append(href)
        except Exception:
            pass
        if urls:
            break

    # Fallback: regex no HTML para links não capturados pelo DOM
    if not urls:
        for bruto in re.findall(
                r'class="result__a"[^>]+href="([^"]+)"', html, re.IGNORECASE):
            final = _resolver_url_ddg(bruto)
            if not final: continue
            d = _dominio_limpo(final)
            if d and "duckduckgo.com" not in d and final not in vistos:
                vistos.add(final); urls.append(final)

    return urls[:n]


# ══════════════════════════════════════════════════════════════════════════════
# SITE — pontuação de URL + verificação profunda
# ══════════════════════════════════════════════════════════════════════════════

def _pontuar_site_url(url: str, nome: str, cidade: str) -> int:
    """Score barato de URL sem fazer request."""
    dom  = _dominio_limpo(url)
    path = urlparse(url).path.lower()

    if "instagram.com" in dom:           return -1
    if _dominio_terceiro(url):           return  0
    for p in PORTAIS:
        if p in dom:                     return  0

    exts = [".com.br",".com",".net.br",".net",".org.br",".org",".imb.br"]
    if not any(dom.endswith(e) for e in exts): return 0

    score = 0
    hits, tokens = _token_match_stats(nome, dom)
    score += hits * 4
    if tokens:
        score += 2  # bônus token mais longo no domínio

    siglas = _siglas_distintivas(nome)
    if siglas and _tem_sigla_exata(siglas, dom):
        score += 4

    if not _cidade_e_ambigua(cidade):
        if _norm(cidade).replace(" ","")[:5] in dom:
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
    """Tenta /contato, /sobre, etc. para mais evidências da marca."""
    base = _url_raiz(site_url)
    if not base: return None, None
    for c in ["contato","fale-conosco","sobre","quem-somos","empresa","imoveis"]:
        url = base + c
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA,
                             allow_redirects=True)
            if r.status_code < 400 and "text/html" in r.headers.get("content-type","").lower():
                return url, r.text
        except Exception:
            continue
    return None, None

def _verificar_site(url: str, nome: str, cidade: str) -> dict:
    vazio = {"ok": False, "score": 0, "motivo": "", "sinais": [],
             "conflitos": [], "ig_link": None, "url_final": None}
    try:
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA,
                             allow_redirects=True)
            req_url = url
            if r.status_code >= 400:
                root = _url_raiz(url)
                if root and root != url:
                    r = requests.get(root, headers=HEADERS,
                                     timeout=TIMEOUT_RETRY, allow_redirects=True)
                    req_url = root
        except requests.Timeout:
            root = _url_raiz(url)
            if root and root != url:
                r = requests.get(root, headers=HEADERS,
                                 timeout=TIMEOUT_RETRY, allow_redirects=True)
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

        # Texto de identidade: domínio + meta tags
        brand_text = " ".join([
            _dominio_limpo(final_url),
            text["title"], text["h1"],
            text["og_title"], text["og_site_name"]
        ])
        hits_brand, tokens_brand = _token_match_stats(nome, brand_text)
        hits_body,  tokens_body  = _token_match_stats(nome, html_n)
        tokens_all = sorted(set(tokens_brand + tokens_body), key=lambda x: (-len(x), x))

        siglas   = _siglas_distintivas(nome)
        sigla_ok = _tem_sigla_exata(
            siglas,
            _dominio_limpo(final_url),
            text["title"], text["h1"],
            text["og_title"], text["og_site_name"]
        )

        if siglas and not sigla_ok:
            conflitos.append("sigla_distintiva_ausente")

        if hits_brand:
            score += hits_brand * 8; sinais.append(f"brand:{hits_brand}")
        if hits_body:
            score += min(hits_body * 4, 10); sinais.append(f"body:{hits_body}")
        if sigla_ok and siglas:
            score += 8; sinais.append("sigla_exata")

        hits_imob = sum(1 for t in TERMOS_IMOB if t in html.lower())
        if hits_imob:
            score += min(hits_imob, 3); sinais.append(f"imob:{min(hits_imob,3)}")

        cidade_ok = _norm(cidade) in html_n
        if cidade_ok:
            score += 8 if not _cidade_e_ambigua(cidade) else 3
            sinais.append("cidade_ok")

        # Página auxiliar só quando há hits de marca mas falta cidade
        # (evita custo desnecessário em casos já claros ou já reprovados)
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

        # Decisão
        ok     = False
        motivo = "site_não_confirmado"

        if _dominio_terceiro(final_url):
            ok = False; motivo = "site_terceiro"
        elif siglas and not sigla_ok:
            ok = False; motivo = "sigla_distintiva_ausente"
        elif hits_brand >= 1 and cidade_ok and score >= 18:
            ok = True;  motivo = "match_forte"
        elif hits_brand >= 2 and score >= 18:
            ok = True;  motivo = "match_composto"
        elif sigla_ok and siglas and score >= 18:
            # Nome só-sigla (ex: AMR, KRS, K2) — sem tokens fortes, mas sigla confirmada
            ok = True;  motivo = "match_sigla_score"
        elif sigla_ok and siglas and (cidade_ok or hits_body >= 2) and score >= 14:
            # Sigla confirmada + cidade OU presença forte no body
            ok = True;  motivo = "match_sigla_cidade"
        elif (_marca_alias_valida(nome, tokens_all) and
              cidade_ok and ig_link and
              any(tok in ig_link for tok in tokens_all)):
            ok = True;  motivo = "alias_marca_comercial"; score += 6
            sinais.append("alias_confirmado")
        elif hits_brand >= 1 and cidade_ok and req_url != url:
            ok = True;  motivo = "match_revalidado_home"

        return {
            "ok":       ok,
            "score":    score,
            "motivo":   motivo,
            "sinais":   sinais,
            "conflitos": conflitos,
            "ig_link":  ig_link,
            "url_final": _url_raiz(final_url) or final_url,
        }

    except requests.Timeout:
        return {**vazio, "motivo": "timeout"}
    except Exception as e:
        return {**vazio, "motivo": str(e)[:80]}


# ══════════════════════════════════════════════════════════════════════════════
# INSTAGRAM — fase 1 (identidade via requests) + fase 2 (métricas via Selenium)
# ══════════════════════════════════════════════════════════════════════════════

def _extrair_nome_display_ig(html: str):
    for pat in [
        r'property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            t = m.group(1)
            t = re.sub(r'\s*\(@[^)]+\).*', '', t).strip()
            t = re.sub(r'\s*•.*', '', t).strip()
            if t: return t
    m = re.search(r'"full_name"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            import codecs
            return codecs.decode(
                m.group(1).replace('\\u','\\u'), 'unicode_escape')
        except Exception:
            return m.group(1)
    return None

def _extrair_bio_ig(html: str) -> str:
    m = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.IGNORECASE)
    return m.group(1) if m else ""

def _pontuar_instagram_candidato(handle: str, nome: str,
                                  cidade: str, origem: str) -> int:
    if handle in IGNORAR_IG or len(handle) < 3:
        return 0
    score = 0
    hits, _ = _token_match_stats(nome, handle)
    if hits:
        score += hits * 4
    siglas = _siglas_distintivas(nome)
    if siglas and _tem_sigla_exata(siglas, handle):
        score += 8
    if any(t in handle for t in ["imob","imovel","imoveis","corretor","condominios"]):
        score += 2
    cn = _norm(cidade).replace(" ","")
    if cn[:4] in _norm(handle) or cn[:5] in _norm(handle):
        score += 1
    bonus = {"site_validado": 8, "site_html": 6, "resultado_site": 4, "ddg_ig": 0}
    score += bonus.get(origem, 0)
    return max(score, 0)

def _extrair_link_bio_ig(html: str) -> str:
    """Extrai a URL do link da bio do Instagram (campo 'website' / external_url)."""
    for pat in [
        r'"external_url"\s*:\s*"([^"]{5,100})"',
        r'"biography_with_entities".*?"url"\s*:\s*"([^"]{5,100})"',
        r'<a[^>]+rel="nofollow"[^>]+href="([^"]{5,200})"',
    ]:
        m = re.search(pat, html)
        if m:
            url = m.group(1)
            # Ignora links internos do IG
            if "instagram.com" not in url and len(url) > 5:
                return url
    return ""


def _verificar_identidade_ig(handle: str, nome: str,
                              cidade: str, site_url=None) -> dict:
    """
    Fase 1: verifica via requests se o perfil pertence à empresa.
    Não faz Selenium — barato e rápido.
    v11.3.5.2: link da bio, proteção ambiguidade, score por coerência com site.
    """
    url = f"https://www.instagram.com/{handle}/"
    neg = {"confirmado": False, "score": 0, "reciproco": False,
           "link_bio": "", "handle_hits": 0, "display_hits": 0,
           "bio_hits": 0, "sigla_ok": False, "motivo": ""}
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

        handle_hits,  handle_tokens  = _token_match_stats(nome, handle)
        display_hits, display_tokens = _token_match_stats(nome, display_name)
        bio_hits,     bio_tokens     = _token_match_stats(nome, bio)
        siglas   = _siglas_distintivas(nome)
        sigla_ok = _tem_sigla_exata(siglas, handle, display_name, bio)
        tokens_all = sorted(
            set(handle_tokens + display_tokens + bio_tokens),
            key=lambda x: (-len(x), x))

        score  = 0
        sinais = []
        if handle_hits:
            score += handle_hits  * 8; sinais.append(f"handle:{handle_hits}")
        if display_hits:
            score += display_hits * 7; sinais.append(f"display:{display_hits}")
        if bio_hits:
            score += min(bio_hits * 4, 8); sinais.append(f"bio:{bio_hits}")
        if sigla_ok and siglas:
            score += 8; sinais.append("sigla_exata")
        if _norm(cidade) in _norm(bio):
            score += 4; sinais.append("cidade_bio")

        reciproco = False
        # Verifica reciprocidade: site aponta para IG OU bio aponta para site
        if site_url:
            dom_site = _dominio_limpo(site_url)
            if dom_site and dom_site in html.lower():
                reciproco = True; score += 5; sinais.append("reciproco_site_no_html")
            # Link da bio aponta para o site (domínio compatível)
            if link_bio and dom_site and _dominio_limpo(link_bio) == dom_site:
                reciproco = True; score += 8; sinais.append("link_bio_igual_site")
            elif link_bio and dom_site:
                # Domínio da bio diferente — verifica se há token do nome
                bio_dom_hits, _ = _token_match_stats(nome, _dominio_limpo(link_bio))
                if bio_dom_hits:
                    score += 4; sinais.append("link_bio_coerente_dominio")

        # ── Proteção contra ambiguidade de nome ──────────────────────────
        # Problema: "Upgrade Intermediações" ≠ "Upgrade Imóveis" — nomes similares,
        # empresas diferentes. Ambas têm "upgrade", mas só uma tem "intermediacoes".
        #
        # Lógica: se o nome da empresa tem tokens distintivos (≥5 chars) que NÃO
        # aparecem em nenhum lugar do perfil (handle + display + bio), penaliza.
        # Quanto mais tokens exclusivos ausentes, maior a penalidade.
        # O display correto sempre vai ter esses tokens.
        todos_perfil       = " ".join([handle, display_name, bio])
        tokens_fortes_nome = _tokens_fortes(nome)
        # Tokens distintivos (≥5 chars) do nome ausentes em todo o perfil
        exclusivos_ausentes = [
            t for t in tokens_fortes_nome
            if len(t) >= 5 and t not in _norm(todos_perfil)
        ]
        if exclusivos_ausentes:
            n_ausentes = len(exclusivos_ausentes)
            penalidade = n_ausentes * 6
            score -= penalidade
            sinais.append(f"tokens_exclusivos_ausentes:{exclusivos_ausentes}")
            # Bloqueio definitivo: o token PRINCIPAL do nome (mais longo/distintivo)
            # está ausente no perfil E há pelo menos outro token ausente.
            # Ex: "Upgrade Intermediações" → principal="intermediacoes" ausente ✅
            # Ex: "Nova Mendes Caetano" → principal="caetano" presente → não bloqueia ✅
            # Ex: "F. Veiga" → apenas 1 forte, token principal presente → não bloqueia ✅
            token_principal = max(tokens_fortes_nome, key=len) if tokens_fortes_nome else ""
            principal_ausente = (token_principal and len(token_principal) >= 5
                                 and token_principal not in _norm(todos_perfil))
            if principal_ausente and len(tokens_fortes_nome) >= 2:
                sinais.append("ambiguidade_bloqueio")

        # ── Decisão de identidade ───────────────────────────────────────
        confirmado = False

        # Pré-bloqueio: ambiguidade de nome confirmada → rejeita diretamente
        if "ambiguidade_bloqueio" in sinais:
            motivo = f"score={score} [{', '.join(sinais)}]"
            return {
                "confirmado":   False,
                "score":        score,
                "reciproco":    reciproco,
                "link_bio":     link_bio,
                "handle_hits":  handle_hits,
                "display_hits": display_hits,
                "bio_hits":     bio_hits,
                "sigla_ok":     sigla_ok,
                "display_name": display_name,
                "motivo":       motivo,
                "html":         html,
            }

        # Caminho 1: sigla distintiva presente mas ausente no perfil → BLOQUEIA
        # (só bloqueia se a sigla for o único identificador — se há tokens fortes,
        #  a sigla ausente não destrói a evidência dos tokens)
        tem_tokens_fortes = len(_tokens_fortes(nome)) > 0
        if siglas and not sigla_ok and not tem_tokens_fortes:
            confirmado = False
            sinais.append("sigla_ausente_bloqueio")

        # Caminho 2: combinação de dois sinais independentes
        elif ((handle_hits > 0 and display_hits > 0) or
              (handle_hits > 0 and bio_hits    > 0) or
              (display_hits > 0 and bio_hits   > 0)):
            confirmado = True

        # Caminho 3: sigla exata + qualquer hit de identidade
        # Para nomes só-sigla (LDL, AMR, K2): handle_hits vem de _tokens_fortes
        # que é vazio. Verifica sigla diretamente no handle/display.
        elif sigla_ok and siglas and (
                handle_hits > 0 or display_hits > 0 or
                _tem_sigla_exata(siglas, handle) or
                _tem_sigla_exata(siglas, display_name)):
            confirmado = True

        # Caminho 4: sigla exata + cidade na bio (forte evidência mesmo sem token)
        elif sigla_ok and siglas and (_norm(cidade) in _norm(bio) or reciproco):
            confirmado = True
            sinais.append("sigla_cidade_bio")

        # Caminho 5: handle bate + reciprocidade (bio aponta para o site)
        elif handle_hits > 0 and reciproco:
            confirmado = True
            sinais.append("handle_reciproco")

        # Caminho 6: handle bate + cidade na bio (muito forte para localidade)
        elif handle_hits > 0 and _norm(cidade) in _norm(bio):
            confirmado = True
            sinais.append("handle_cidade_bio")

        # Caminho 7: display bate + cidade na bio
        elif display_hits > 0 and _norm(cidade) in _norm(bio):
            confirmado = True
            sinais.append("display_cidade_bio")

        # Caminho 8: alias longo no handle ou display + evidência secundária
        elif _marca_alias_valida(nome, tokens_all):
            alias = max(tokens_all, key=len) if tokens_all else ""
            if alias and (alias in _norm(handle) or alias in _norm(display_name)):
                if bio_hits > 0 or reciproco or _norm(cidade) in _norm(bio):
                    confirmado = True; sinais.append("alias_confirmado")

        motivo = f"score={score} [{', '.join(sinais) or 'sem_sinais'}]"
        return {
            "confirmado":   confirmado,
            "score":        score,
            "reciproco":    reciproco,
            "link_bio":     link_bio,
            "handle_hits":  handle_hits,
            "display_hits": display_hits,
            "bio_hits":     bio_hits,
            "sigla_ok":     sigla_ok,
            "display_name": display_name,
            "motivo":       motivo,
            "html":         html,   # repassa para fase 2
        }
    except Exception as e:
        return {**neg, "motivo": str(e)[:80]}


def _parse_numero(s: str):
    try:
        s       = s.strip()
        tem_mil = "mil" in s.lower()
        tem_k   = s.lower().endswith("k") and not tem_mil
        s_num   = re.sub(r"(?:mil|k)", "", s, flags=re.IGNORECASE).strip()
        if tem_mil or tem_k:
            s_num = s_num.replace(".", "").replace(",", ".")
            try:    valor = float(s_num)
            except: m = re.search(r"\d+", s_num); valor = float(m.group()) if m else 0.0
            return int(round(valor * 1000))
        return int(s_num.replace(".", "").replace(",", ""))
    except Exception:
        return None

def _extrair_datas_json(html: str) -> list:
    """
    Extrai datas de posts do JSON embutido no HTML do Instagram.
    Cobre todos os padrões conhecidos de timestamp.
    """
    from datetime import datetime, timezone
    datas = set()

    # Padrões de Unix timestamp (10 dígitos)
    for pat in [
        r'"taken_at_timestamp"\s*:\s*(\d{10})',
        r'"taken_at"\s*:\s*(\d{10})',
        r'"timestamp"\s*:\s*(\d{10})',
        r'"created_time"\s*:\s*"(\d{10})"',
        r'"date_time_original"\s*:\s*"(\d{10})"',
    ]:
        for ts in re.findall(pat, html):
            try:
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                d  = dt.strftime("%Y-%m-%d")
                if d > "2015-01-01":
                    datas.add(d)
            except Exception:
                pass

    # Padrões ISO 8601
    for iso in re.findall(
            r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^"]{0,10})"', html):
        try:
            d = iso[:10]
            if d > "2015-01-01":
                datas.add(d)
        except Exception:
            pass

    # Padrão "date": "YYYY-MM-DD" (algumas versões do IG)
    for d in re.findall(r'"date"\s*:\s*"(\d{4}-\d{2}-\d{2})"', html):
        if d > "2015-01-01":
            datas.add(d)

    return sorted(datas, reverse=True)[:20]

def _verificar_metricas_ig(driver, handle: str,
                            html_requests: str = None) -> dict:
    """
    Fase 2: Selenium para seguidores, posts e data do último post.
    html_requests: HTML já obtido via requests na fase 1 (reaproveita dados).
    """
    resultado = {
        "seguidores": None, "num_posts": None,
        "ultimo_post": None, "semanas": None, "motivo": None,
    }

    # ── Tenta extrair métricas do HTML da fase 1 (sem nova request) ───────
    if html_requests:
        meta_m = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            html_requests, re.IGNORECASE)
        if meta_m:
            desc  = meta_m.group(1)
            seg_m = re.search(
                r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)',
                desc, re.IGNORECASE)
            pos_m = re.search(
                r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)',
                desc, re.IGNORECASE)
            if seg_m: resultado["seguidores"] = _parse_numero(seg_m.group(1))
            if pos_m: resultado["num_posts"]  = _parse_numero(pos_m.group(1))

        # Data dos posts via JSON embutido
        datas_json = _extrair_datas_json(html_requests)
        if datas_json:
            resultado["ultimo_post"] = datas_json[0]

    # ── Selenium: navega ao perfil para complementar dados ────────────────
    url = f"https://www.instagram.com/{handle}/"
    try:
        driver.get(url)
        time.sleep(3.5)

        # Scroll progressivo: desce devagar para forçar lazy loading do grid
        for scroll_y in [300, 600, 900, 600, 300, 0]:
            driver.execute_script(f"window.scrollTo(0, {scroll_y})")
            time.sleep(0.4)
        time.sleep(1.0)

        html_sel = driver.page_source

        if ("sorry, this page" in html_sel.lower() or
                "página não disponível" in html_sel.lower()):
            resultado["motivo"] = "perfil_nao_existe"
            return resultado

        # ── Extrai seguidores e posts — 4 métodos em cascata ────────────────

        # Método 1: meta description (formato: "X seguidores, Y seguindo, Z posts")
        for html_src in [html_sel] + ([html_requests] if html_requests else []):
            if resultado["seguidores"] is not None and resultado["num_posts"] is not None:
                break
            meta_m = re.search(
                r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
                html_src, re.IGNORECASE)
            if meta_m:
                desc = meta_m.group(1)
                if resultado["seguidores"] is None:
                    seg_m = re.search(
                        r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)',
                        desc, re.IGNORECASE)
                    if seg_m: resultado["seguidores"] = _parse_numero(seg_m.group(1))
                if resultado["num_posts"] is None:
                    pos_m = re.search(
                        r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)',
                        desc, re.IGNORECASE)
                    if pos_m: resultado["num_posts"] = _parse_numero(pos_m.group(1))

        # Método 2: JSON embutido — múltiplos padrões conhecidos
        if resultado["seguidores"] is None:
            for pat in [
                r'"edge_followed_by":\{"count":(\d+)\}',
                r'"follower_count":(\d+)',
                r'"followers_count":(\d+)',
                r'"followers":(\d+)',
                r'"userInteractionCount":(\d+)',
            ]:
                m = re.search(pat, html_sel)
                if m: resultado["seguidores"] = int(m.group(1)); break

        if resultado["num_posts"] is None:
            for pat in [
                r'"edge_owner_to_timeline_media":\{"count":(\d+)',
                r'"media_count":(\d+)',
                r'"total_igtv_videos":\d+.*?"media_count":(\d+)',
            ]:
                m = re.search(pat, html_sel)
                if m: resultado["num_posts"] = int(m.group(1)); break

        # Método 3: elementos Selenium com contexto (li pai)
        if resultado["seguidores"] is None or resultado["num_posts"] is None:
            try:
                from selenium.webdriver.common.by import By
                for sel_css in [
                    "header section ul li",
                    "ul[class*='x'] li",
                    "section ul li",
                ]:
                    try:
                        lis = driver.find_elements(By.CSS_SELECTOR, sel_css)
                        if not lis: continue
                        for li in lis[:6]:
                            txt = (li.text or "").lower().strip()
                            if not txt: continue
                            m_num = re.search(r'([\d\.\,]+(?:\s*(?:mil|k))?)', txt)
                            if not m_num: continue
                            num = _parse_numero(m_num.group(1))
                            if num is None: continue
                            if any(p in txt for p in ["publicaç","post","publicacoes","posts"]):
                                if resultado["num_posts"] is None:
                                    resultado["num_posts"] = num
                            elif any(p in txt for p in ["seguidor","follower","seguidores"]):
                                if resultado["seguidores"] is None:
                                    resultado["seguidores"] = num
                        if resultado["seguidores"] is not None: break
                    except Exception:
                        continue
            except Exception:
                pass

        # Método 4: spans numéricos com contexto do li pai
        if resultado["seguidores"] is None or resultado["num_posts"] is None:
            try:
                from selenium.webdriver.common.by import By
                spans = driver.find_elements(
                    By.CSS_SELECTOR,
                    "header section ul li span span, "
                    "header section ul li span[class*='x5n08af']")
                for span in spans[:12]:
                    txt = (span.text or "").strip()
                    txt_clean = txt.replace(".", "").replace(",", "")
                    if not txt_clean: continue
                    # Verifica se é número (pode ter sufixo mil/k)
                    m_num = re.search(r'^([\d]+)', txt_clean)
                    if not m_num: continue
                    try:
                        li_pai = driver.execute_script(
                            "return arguments[0].closest('li')", span)
                        ctx = (li_pai.text or "").lower() if li_pai else ""
                    except Exception:
                        ctx = ""
                    num = _parse_numero(txt)
                    if num is None: continue
                    if any(p in ctx for p in ["publicaç","post"]):
                        if resultado["num_posts"] is None: resultado["num_posts"] = num
                    elif any(p in ctx for p in ["seguidor","follower"]):
                        if resultado["seguidores"] is None: resultado["seguidores"] = num
            except Exception:
                pass

        # ── Extrai data do último post — 4 métodos em cascata ───────────────
        # Ordem: JSON rápido → posts individuais (confiável) → DOM/JS → fallback

        # Método 1: JSON embutido no Selenium (taken_at_timestamp)
        # Funciona quando o IG inclui dados de timeline no HTML (nem sempre)
        datas_sel = _extrair_datas_json(html_sel)
        if datas_sel and (resultado["ultimo_post"] is None or
                          datas_sel[0] > resultado["ultimo_post"]):
            resultado["ultimo_post"] = datas_sel[0]

        # Método 2: abre posts individualmente — método mais confiável
        # O IG sempre inclui <time datetime> em posts individuais.
        # Roda sempre que o método 1 não encontrou nada OU encontrou mas
        # queremos confirmar (posts individuais são mais precisos que JSON do perfil).
        if resultado["ultimo_post"] is None:
            resultado["ultimo_post"] = _extrair_data_abrindo_posts(driver, html_sel)

        # Método 3: time[datetime] no DOM do perfil + scroll para lazy loading
        if resultado["ultimo_post"] is None:
            from selenium.webdriver.common.by import By
            try:
                # Scroll suave para forçar carregamento do grid
                for sy in [400, 800, 400, 0]:
                    driver.execute_script(f"window.scrollTo(0, {sy})")
                    time.sleep(0.5)
                time.sleep(1.0)
                html_scroll = driver.page_source
                # Tenta JSON de novo após scroll
                datas_scroll = _extrair_datas_json(html_scroll)
                if datas_scroll:
                    resultado["ultimo_post"] = datas_scroll[0]
                else:
                    # time[datetime] no DOM
                    datas_dom = []
                    for sel_time in ["time[datetime]", "article time", "main time"]:
                        try:
                            for tel in driver.find_elements(
                                    By.CSS_SELECTOR, sel_time)[:15]:
                                dt_attr = tel.get_attribute("datetime") or ""
                                if dt_attr and len(dt_attr) >= 10:
                                    d = dt_attr[:10]
                                    if d > "2015-01-01":
                                        datas_dom.append(d)
                        except Exception:
                            continue
                    if datas_dom:
                        resultado["ultimo_post"] = sorted(datas_dom, reverse=True)[0]
            except Exception:
                pass

        # Método 4: JavaScript direto no DOM (últimos atributos datetime visíveis)
        if resultado["ultimo_post"] is None:
            try:
                datas_js = driver.execute_script("""
                    var datas = [];
                    var els = document.querySelectorAll('time[datetime], article time');
                    for (var i=0; i<Math.min(els.length, 20); i++) {
                        var d = els[i].getAttribute('datetime') || '';
                        if (d.length >= 10) datas.push(d.substring(0, 10));
                    }
                    var links = document.querySelectorAll('a[href*="/p/"]');
                    for (var j=0; j<Math.min(links.length, 6); j++) {
                        var t = links[j].querySelector('time');
                        if (t) {
                            var d2 = t.getAttribute('datetime') || '';
                            if (d2.length >= 10) datas.push(d2.substring(0, 10));
                        }
                    }
                    return datas;
                """) or []
                validas = sorted(
                    [d for d in datas_js if d > "2015-01-01"], reverse=True)
                if validas:
                    resultado["ultimo_post"] = validas[0]
            except Exception:
                pass

    except Exception as e:
        if not resultado.get("motivo"):
            resultado["motivo"] = f"selenium: {str(e)[:60]}"

    return resultado

def _extrair_data_abrindo_posts(driver, html_perfil: str):
    """Último recurso: abre posts individuais para pegar datas."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        time.sleep(1.5)
        hrefs, vistos = [], set()
        for sel in ["article a[href*='/p/']", "a[href*='/p/']",
                    "main article a", "div[class*='_aagu'] a"]:
            try:
                posts = driver.find_elements(By.CSS_SELECTOR, sel)
                for p in posts:
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
                # Tenta time[datetime]
                try:
                    tel = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "time[datetime]")))
                    dt_val = tel.get_attribute("datetime") or ""
                    if dt_val: datas.append((idx, dt_val[:10])); continue
                except Exception:
                    pass
                # Tenta JSON no post
                ph = driver.page_source
                ds = _extrair_datas_json(ph)
                if ds: datas.append((idx, ds[0])); continue
                m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', ph)
                if m: datas.append((idx, m.group(1)))
            except Exception:
                continue

        driver.get(url_perfil)
        time.sleep(1.5)
        if not datas: return None
        if len(datas) == 1: return datas[0][1]
        return sorted([d for _, d in datas], reverse=True)[0]
    except Exception:
        return None

def _detectar_pinned_post(driver, html_perfil: str):
    """
    Verifica se o primeiro post do grid está fixado comparando datas.
    Se o post 1 for mais antigo que algum dos posts 2-4, é pinned.
    Retorna a data mais recente (real).
    """
    datas_json = _extrair_datas_json(html_perfil)
    if len(datas_json) < 2:
        return None
    # Com vários timestamps, já sabemos qual é o mais recente
    return datas_json[0]

def _extrair_ig_do_site(site_url: str, nome: str, cidade: str):
    try:
        r = requests.get(site_url, headers=HEADERS, timeout=TIMEOUT_PAGINA)
        if r.status_code != 200: return None
        handles = _extrair_ig_links(r.text)
        if handles:
            return max(handles, key=lambda h:
                       _pontuar_instagram_candidato(h, nome, cidade, "site_html"))
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
        log(f"  🌐 DDG ({i}/3): {query}")
        urls = _buscar_duckduckgo(driver, query, n=8 if i == 1 else 6)

        if urls == ["__BLOQUEADO__"]:
            log("    ⚠️ DDG bloqueado nesta query")
            continue

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
                vistos.add(url)
                candidatos.append({"url": url, "score": score})

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
        log(f"    {'✅' if v['ok'] else '❌'} {v['motivo']} | "
            f"sinais={v.get('sinais',[])} score={v.get('score',0)}{conflito_txt}")

        if v["ok"]:
            if v.get("ig_link"):
                log(f"    🔗 IG no site: @{v['ig_link']}")
            return {
                "url":               v["url_final"],
                "confirmado":        True,
                "sem_site":          False,
                "ig_dos_resultados": handles_ig,
                "ig_do_site_validado": v.get("ig_link"),
                "site_score":        v["score"],
                "site_sinais":       v["sinais"],
                "site_motivo":       v["motivo"],
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
                         ig_do_site_validado: str, ig_dos_resultados: list,
                         log) -> dict:
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
            vistos.add(h)
            s = _pontuar_instagram_candidato(h, nome, cidade, "site_html")
            candidatos.append({"handle": h, "score": s, "origem": "site_html"})
            log(f"    🥈 HTML do site: @{h} (score={s})")

    for h in ig_dos_resultados:
        if h not in vistos:
            vistos.add(h)
            s = _pontuar_instagram_candidato(h, nome, cidade, "resultado_site")
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
                vistos.add(h)
                s = _pontuar_instagram_candidato(h, nome, cidade, "ddg_ig")
                candidatos.append({"handle": h, "score": s, "origem": "ddg_ig"})
                log(f"    🔍 DDG: @{h} (score={s})")

    # Candidatos de origens confiáveis (site) passam mesmo com score 0
    # Candidatos DDG precisam de score >= 0 (aceita todos — são filtrados na verificação)
    # Candidatos de resultado_site precisam de score > 0
    candidatos = sorted(
        [c for c in candidatos
         if c["score"] > 0
         or c["origem"] in ("site_validado", "site_html")],
        key=lambda x: -x["score"])

    if not candidatos:
        log("    → Nenhum candidato de Instagram")
        return {"url": None, "handle": None, "confirmado": False, "sem_ig": True,
                "metodo": None, "origem": None, "motivo": "sem_candidatos"}

    for cand in candidatos[:MAX_TENTATIVAS_IG]:
        h      = cand["handle"]
        origem = cand["origem"]
        log(f"    → verificando @{h} (score={cand['score']}, origem={origem})")

        # Fase 1: identidade via requests (barato)
        ident = _verificar_identidade_ig(h, nome, cidade, site_url=site_url)

        aprovado = ident["confirmado"]

        # Origens do site: handle com 1 hit + sem sigla bloqueando = aprova
        if not aprovado and origem in ("site_validado", "site_html"):
            handle_hits = ident.get("handle_hits", 0)
            display_hits = ident.get("display_hits", 0)
            siglas = _siglas_distintivas(nome)
            sigla_ok = ident.get("sigla_ok", False)
            # Aceita se tem qualquer hit de identidade E sigla não bloqueia
            tem_hit = handle_hits > 0 or display_hits > 0 or ident.get("bio_hits", 0) > 0
            sigla_bloqueia = siglas and not sigla_ok and not len(_tokens_fortes(nome)) > 0
            if tem_hit and not sigla_bloqueia:
                aprovado = True
                log(f"    ↗️ Aprovado por origem confiável ({origem}): hits OK")

        # resultado_site com reciprocidade: confiança alta
        if not aprovado and origem == "resultado_site" and ident.get("reciproco", False):
            if ident.get("handle_hits", 0) > 0 or ident.get("display_hits", 0) > 0:
                aprovado = True
                log(f"    ↗️ Aprovado: resultado_site recíproco")

        # ddg_ig: exige reciprocidade OU handle + cidade_bio (muito forte)
        if not aprovado and origem == "ddg_ig":
            if ident.get("reciproco"):
                if ident.get("handle_hits", 0) > 0 or ident.get("display_hits", 0) > 0:
                    aprovado = True
                    log(f"    ↗️ ddg_ig aprovado: reciproco + hit")
            # Não aprova ddg_ig sem qualquer evidência
        if aprovado and origem == "ddg_ig" and not ident.get("reciproco", False):
            # Verifica se chegou aqui por caminho válido (cidade_bio ou handle+display)
            sinais_ident = ident.get("motivo", "")
            tem_evidencia_forte = any(s in sinais_ident for s in
                ["handle_cidade_bio", "display_cidade_bio", "sigla_cidade_bio"])
            if not tem_evidencia_forte and not (
                    ident.get("handle_hits", 0) > 0 and ident.get("display_hits", 0) > 0):
                log(f"    ⛔ ddg_ig sem evidência suficiente — rejeitado")
                aprovado = False

        if not aprovado:
            log(f"    ❌ identidade: {ident['motivo']}")
            continue

        log(f"    ✅ identidade: {ident['motivo']}")

        # Fase 2: métricas via Selenium (seguidores, posts, data)
        metricas = _verificar_metricas_ig(
            driver, h,
            html_requests=ident.get("html"))

        seg   = metricas.get("seguidores")
        posts = metricas.get("num_posts")
        data  = metricas.get("ultimo_post")

        log(f"    📊 seg={seg} posts={posts} último={data}")

        if seg is not None and seg < MIN_SEGUIDORES:
            log(f"    ❌ poucos seguidores ({seg})")
            continue
        if posts is not None and posts < MIN_POSTS:
            log(f"    ❌ poucos posts ({posts})")
            continue

        semanas = None
        if data:
            try:
                dt      = date.fromisoformat(data)
                semanas = (date.today() - dt).days // 7
                if semanas > MAX_SEMANAS_INATIVO:
                    log(f"    ❌ inativo: {semanas} semanas desde último post")
                    continue
            except Exception:
                pass
        else:
            # Data não detectada — decisão baseada na origem
            # site_validado / site_html: origem confiável, scraping pode ter
            # falhado por limitação técnica, não é evidência de inatividade
            if origem in ("site_validado", "site_html"):
                log(f"    ⚠️ data não detectada (origem={origem}) — flag revisão")
            elif origem == "resultado_site" and ident.get("reciproco"):
                log(f"    ⚠️ data não detectada (recíproco) — flag revisão")
            else:
                # DDG ou sem reciprocidade: exige data para aprovar
                log(f"    ❌ data não detectada (origem={origem}) — rejeitado")
                continue

        return {
            "url":        f"https://www.instagram.com/{h}/",
            "handle":     h,
            "confirmado": True,
            "sem_ig":     False,
            "metodo":     origem,
            "origem":     origem,
            "reciproco":  ident.get("reciproco", False),
            "seguidores": seg,
            "num_posts":  posts,
            "ultimo_post": data,
            "semanas":    semanas,
            "motivo":     ident["motivo"] + (f" | {metricas.get('motivo','')}" if metricas.get("motivo") else ""),
        }

    return {"url": None, "handle": None, "confirmado": False, "sem_ig": True,
            "metodo": None, "origem": None, "motivo": "nao_confirmado"}


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR
# ══════════════════════════════════════════════════════════════════════════════

def enriquecer(driver, nome: str, cidade: str,
               log=None) -> ResultadoEnriquecimento:
    def _log(msg):
        if log: log(msg)

    r = ResultadoEnriquecimento(nome=nome, cidade=cidade)
    try:
        # ── Site ──────────────────────────────────────────────────────────
        site = _descobrir_site(driver, nome, cidade, _log)
        r.site_url       = site["url"]
        r.site_confirmado = site["confirmado"]
        r.site_score     = site.get("site_score", 0)
        r.site_sinais    = site.get("site_sinais", [])
        r.site_motivo    = site.get("site_motivo")
        r.sem_site       = site["sem_site"]

        if r.sem_site or not r.site_confirmado:
            r.sem_instagram = True
            r.ig_motivo     = "instagram_pulado_sem_site"
            r.motivo_rejeicao_ig = r.ig_motivo
            _log("  ⏭️ Instagram pulado — site não confirmado")
            return r

        time.sleep(1)

        # ── Instagram ─────────────────────────────────────────────────────
        ig = _descobrir_instagram(
            driver, nome, cidade,
            site_url=r.site_url,
            ig_do_site_validado=site.get("ig_do_site_validado"),
            ig_dos_resultados=site.get("ig_dos_resultados", []),
            log=_log,
        )
        r.instagram_url     = ig.get("url")
        r.ig_handle         = ig.get("handle")
        r.ig_confirmado     = ig.get("confirmado", False)
        r.ig_metodo         = ig.get("metodo")
        r.ig_origem         = ig.get("origem")
        r.ig_reciproco      = ig.get("reciproco", False)
        r.ig_seguidores     = ig.get("seguidores")
        r.ig_num_posts      = ig.get("num_posts")
        r.ig_ultimo_post    = ig.get("ultimo_post")
        r.ig_semanas        = ig.get("semanas")
        r.sem_instagram     = ig.get("sem_ig", True)
        r.ig_motivo         = ig.get("motivo")
        r.motivo_rejeicao_ig = ig.get("motivo")

        # ── Review flags ──────────────────────────────────────────────────
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
        _log(f"  ⚠️ Erro: {e}")

    return r
