"""
agents/enrichment_agent.py — Enriquecimento de site + Instagram.
v11.3-ddg — mesma lógica da v11_3, trocando Google por DuckDuckGo na descoberta.
"""

import re, time, requests
from datetime import date
from urllib.parse import urlparse, quote
from dataclasses import dataclass

# ── Critérios mínimos Instagram ───────────────────────────────────────────────
MIN_SEGUIDORES      = 500
MIN_POSTS           = 20
MAX_SEMANAS_INATIVO = 8

MAX_TENTATIVAS_SITE = 2
MAX_TENTATIVAS_IG   = 2

TIMEOUT_PAGINA = 10

# Cidades cujo nome coincide com o do estado — query precisa de UF
CIDADES_AMBIGUAS = {"saopaulo": "SP", "riodejaneiro": "RJ"}

PORTAIS = [
    "zapimoveis","vivareal","olx","quintoandar","chavesnamao",
    "imovelweb","wimoveis","netimoveis","123imoveis","imobiliare",
    "facebook","linkedin","twitter","youtube","tiktok",
    "google","bing","yahoo","gstatic","googleapis",
    "cnpj.biz","cnpja","receitaws","minhareceita","econodata",
    "jusbrasil","escavador","solutudo","telelista","apontador",
    "guiamais","listel","procuroacho","advdinamico","empresaqui",
    "infobel","serasaexperian","serasa","boavista","spc",
    "convergencia","avaliacoesdeareas","reclameaqui",
]

IGNORAR_IG = {"p","reel","reels","stories","explore","tv",
              "accounts","instagram","about","sharedfiles","highlights"}

TERMOS_IMOB = [
    "imóveis","imoveis","imobiliária","imobiliaria","corretor",
    "corretora","venda","locação","locacao","aluguel",
    "apartamento","casa","terreno","comprar imóvel",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


@dataclass
class ResultadoEnriquecimento:
    nome:               str
    cidade:             str
    site_url:           str  = None
    site_confirmado:    bool = False
    site_score:         int  = 0
    site_sinais:        list = None
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
    sem_instagram:      bool = False
    motivo_rejeicao_ig: str  = None
    erro:               str  = None
    review_flags:       list = None

    def __post_init__(self):
        if self.site_sinais is None:
            self.site_sinais = []
        if self.review_flags is None:
            self.review_flags = []


def _extrair_nome_display_ig(html: str) -> str | None:
    for pat in [
        r'property=["\\\']og:title["\\\'][^>]+content=["\\\']([^"\\\']+)["\\\']',
        r'content=["\\\']([^"\\\']+)["\\\'][^>]+property=["\\\']og:title["\\\']',
    ]:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            title = m.group(1)
            display = re.sub(r'\s*\(@[^)]+\).*', '', title).strip()
            display = re.sub(r'\s*•.*', '', display).strip()
            if display and len(display) > 2:
                return display

    m = re.search(r'"full_name"\s*:\s*"([^"]+)"', html)
    if m:
        try:
            import codecs
            return codecs.decode(m.group(1).replace('\\u', '\\u'), 'unicode_escape')
        except Exception:
            return m.group(1)
    return None


def _nomes_compatíveis(nome_empresa: str, nome_perfil: str) -> bool:
    palavras_emp = set(_palavras(nome_empresa))
    palavras_perfil = set(_palavras(nome_perfil))

    if not palavras_emp or not palavras_perfil:
        return False

    if palavras_emp & palavras_perfil:
        return True

    for p_emp in palavras_emp:
        if len(p_emp) <= 4 and p_emp in _norm(nome_perfil).replace(" ", ""):
            return True

    for p_perf in palavras_perfil:
        if len(p_perf) <= 4 and p_perf in _norm(nome_empresa).replace(" ", ""):
            return True

    return False


def _extrair_cidade_do_perfil_ig(html: str) -> str | None:
    meta_m = re.search(
        r'<meta[^>]+name=["\\\']description["\\\'][^>]+content=["\\\']([^"\\\']+)["\\\']',
        html, re.IGNORECASE
    )
    if meta_m:
        desc = meta_m.group(1)
        city_m = re.search(
            r',\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]{2,30}),\s*(?:Brazil|Brasil)',
            desc
        )
        if city_m:
            return city_m.group(1).strip()

    city_m2 = re.search(
        r'([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]{2,30}),\s*(?:Brazil|Brasil)\s*(?:\d{5}|<)',
        html
    )
    if city_m2:
        return city_m2.group(1).strip()
    return None


def _cidade_conflito(cidade_encontrada: str, cidade_esperada: str) -> bool:
    if not cidade_encontrada:
        return False
    norm_enc = _norm(cidade_encontrada).replace(" ", "")
    norm_esp = _norm(cidade_esperada).replace(" ", "")
    if norm_enc == norm_esp:
        return False
    if norm_enc in norm_esp or norm_esp in norm_enc:
        return False
    return True


def _norm(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in s if not unicodedata.combining(c))


def _palavras(nome: str) -> list:
    ignorar = {
        "ltda","eireli","me","sa","s/a","epp","ss",
        "e","de","da","do","das","dos","em","com","para","por","a","o","as","os",
        "imobiliaria","imoveis","imovel","corretora","corretor",
        "consultoria","negocios","servicos",
    }
    return [p for p in re.split(r'\W+', _norm(nome))
            if len(p) > 1 and p not in ignorar]


def _dominio_simples(url: str) -> str | None:
    try:
        d = urlparse(url if url.startswith("http") else "https://" + url).netloc
        return d.lower().replace("www.", "") or None
    except Exception:
        return None


def _cidade_query(cidade: str) -> str:
    cidade_norm = _norm(cidade).replace(" ", "")
    uf = CIDADES_AMBIGUAS.get(cidade_norm)
    if uf:
        return f"{cidade} {uf}"
    return cidade


def _cidade_e_ambigua(cidade: str) -> bool:
    return _norm(cidade).replace(" ", "") in CIDADES_AMBIGUAS


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
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def _resolver_url_ddg(url: str) -> str | None:
    from urllib.parse import parse_qs, unquote

    if not url:
        return None

    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://duckduckgo.com" + url

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
        qs = parse_qs(parsed.query or "")
        uddg = qs.get("uddg", [None])[0]
        if uddg:
            return unquote(uddg)
        return None

    if parsed.scheme in ("http", "https") and parsed.netloc:
        return url

    return None


def _buscar_duckduckgo(driver, query: str, n: int = 10) -> list:
    url = f"https://duckduckgo.com/html/?q={quote(query)}&kl=br-pt"
    driver.get(url)
    time.sleep(2.0)
    html = driver.page_source

    urls, vistos = [], set()

    padroes = [
        r'class="result__a"[^>]+href="([^"]+)"',
        r'nofollow" class="result__a" href="([^"]+)"',
        r'<a[^>]+href="([^"]+)"[^>]*class="[^"]*result__a[^"]*"',
    ]

    for padrao in padroes:
        for bruto in re.findall(padrao, html, flags=re.IGNORECASE):
            final = _resolver_url_ddg(bruto)
            if not final:
                continue

            dom = urlparse(final).netloc.lower()
            if not dom or "duckduckgo.com" in dom:
                continue

            if final not in vistos:
                vistos.add(final)
                urls.append(final)
            if len(urls) >= n:
                return urls[:n]

    return urls[:n]


def _pontuar_site(url: str, nome: str, cidade: str) -> int:
    dominio = urlparse(url).netloc.lower().replace("www.", "")
    path = urlparse(url).path.lower()

    if "instagram.com" in dominio:
        return -1

    for p in PORTAIS:
        if p in dominio:
            return 0
    extensoes = ['.com.br','.com','.net.br','.net','.org.br','.org','.imb.br']
    if not any(dominio.endswith(e) for e in extensoes):
        return 0
    if any(x in dominio for x in ["gstatic","cloudfront","amazonaws","cdn"]):
        return 0

    score = 0
    palavras = _palavras(nome)
    hits = sum(1 for p in palavras if p in dominio)
    score += hits * 3

    if not _cidade_e_ambigua(cidade):
        if _norm(cidade).replace(" ","")[:5] in dominio:
            score += 1

    if dominio.endswith('.com.br'):
        score += 1
    if path.count('/') > 3:
        score -= 1

    return max(score, 0)


def _verificar_site(url: str, nome: str, cidade: str) -> dict:
    vazio = {"ok": False, "score": 0, "motivo": "", "sinais_fortes": [], "ig_link": None}
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA, allow_redirects=True)
        if r.status_code >= 400:
            return {**vazio, "motivo": f"HTTP {r.status_code}"}

        dom_final = urlparse(r.url).netloc.lower()
        for p in PORTAIS:
            if p in dom_final:
                return {**vazio, "motivo": f"redirecionou para portal ({p})"}

        html = r.text
        html_l = html.lower()
        palavras = _palavras(nome)
        score = 0
        sinais_fortes = []
        sinais_medios = []
        ig_link = None

        title_m = re.search(r'<title[^>]*>([^<]{2,120})</title>', html, re.IGNORECASE)
        if title_m:
            title_norm = _norm(title_m.group(1))
            if sum(1 for p in palavras if p in title_norm) >= 1:
                score += 20
                sinais_fortes.append("nome no <title>")

        h1_m = re.search(r'<h1[^>]*>([^<]{2,120})</h1>', html, re.IGNORECASE)
        if h1_m:
            h1_norm = _norm(h1_m.group(1))
            if sum(1 for p in palavras if p in h1_norm) >= 1:
                score += 15
                sinais_fortes.append("nome no <h1>")

        ogtitle_m = None
        for pat in [
            r'property=["\\\']og:title["\\\'][^>]+content=["\\\']([^"\\\']+)["\\\']',
            r'content=["\\\']([^"\\\']+)["\\\'][^>]+property=["\\\']og:title["\\\']',
        ]:
            ogtitle_m = re.search(pat, html, re.IGNORECASE)
            if ogtitle_m:
                break
        if ogtitle_m:
            ogtitle_norm = _norm(ogtitle_m.group(1))
            palavras_titulo = set(re.split(r'\W+', ogtitle_norm)) - {"", "de", "da", "do", "e"}
            palavras_emp = set(_palavras(nome))
            if len(palavras_titulo) >= 2 and len(palavras_emp) >= 1:
                if not (palavras_titulo & palavras_emp):
                    score -= 20
                    sinais_medios.append("og:title incompatível")

        for pat in [
            r'property=["\\\']og:site_name["\\\'][^>]+content=["\\\']([^"\\\']{2,80})["\\\']',
            r'content=["\\\']([^"\\\']{2,80})["\\\'][^>]+property=["\\\']og:site_name["\\\']',
        ]:
            og_m = re.search(pat, html, re.IGNORECASE)
            if og_m:
                og_norm = _norm(og_m.group(1))
                if sum(1 for p in palavras if p in og_norm) >= 1:
                    score += 15
                    sinais_fortes.append("og:site_name")
                break

        ig_m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', html)
        if ig_m:
            h = ig_m.group(1).lower()
            if h not in IGNORAR_IG:
                ig_link = h
                score += 10
                sinais_fortes.append(f"link IG @{h}")

        if title_m:
            title_txt = title_m.group(1)
            from config import CIDADES_POPULACAO
            cidade_norm_esp = _norm(cidade).replace(" ", "")
            for cid_conhecida in CIDADES_POPULACAO:
                cid_norm = _norm(cid_conhecida).replace(" ", "")
                if cid_norm == cidade_norm_esp or len(cid_norm) < 4:
                    continue
                if cid_norm in _norm(title_txt):
                    return {
                        "ok": False, "score": 0, "ig_link": ig_link,
                        "sinais_fortes": sinais_fortes,
                        "motivo": f"cidade incorreta no título: {cid_conhecida} ≠ {cidade}",
                    }

        hits_nome = sum(1 for p in palavras if p in html_l)
        if hits_nome >= 2:
            score += 10
            sinais_medios.append(f"nome HTML ({hits_nome}×)")
        elif hits_nome == 1:
            score += 5
            sinais_medios.append("nome HTML (1×)")

        cidade_norm = _norm(cidade)
        cidade_no_html = cidade_norm in _norm(html)
        if cidade_no_html:
            if _cidade_e_ambigua(cidade):
                score += 2
                sinais_medios.append("cidade (ambígua)")
            else:
                score += 8
                sinais_medios.append("cidade")

        hits_imob = sum(1 for t in TERMOS_IMOB if t in html_l)
        if hits_imob >= 3:
            score += 6
            sinais_medios.append(f"termos imob ({hits_imob})")
        elif hits_imob >= 1:
            score += 3

        tem_forte = len(sinais_fortes) > 0

        if not tem_forte:
            return {
                "ok": False, "score": score, "ig_link": ig_link,
                "sinais_fortes": sinais_fortes,
                "motivo": f"sem sinal forte (score={score}) | médios: {sinais_medios}",
            }
        if score < 20:
            return {
                "ok": False, "score": score, "ig_link": ig_link,
                "sinais_fortes": sinais_fortes,
                "motivo": f"score insuficiente ({score}) | fortes: {sinais_fortes}",
            }

        return {
            "ok": True, "score": score, "ig_link": ig_link,
            "sinais_fortes": sinais_fortes,
            "motivo": f"score={score} | fortes: {sinais_fortes} | médios: {sinais_medios}",
        }

    except requests.Timeout:
        return {**vazio, "motivo": "timeout"}
    except Exception as e:
        return {**vazio, "motivo": str(e)[:60]}


def _pontuar_ig(handle: str, nome: str, cidade: str, origem: str = "google_ig") -> int:
    if handle in IGNORAR_IG or len(handle) < 3:
        return 0
    h = handle.lower()
    palavras = _palavras(nome)
    score = 0

    hits = sum(1 for p in palavras if p in h)

    if hits == 0 and len(palavras) >= 1:
        score -= 6
    else:
        score += hits * 3

    termos_ig = ["imob","imovel","imoveis","corretor","corretora",
                 "residencial","incorpor","casas","aptos","apartamentos"]
    if any(t in h for t in termos_ig):
        score += 2

    if not _cidade_e_ambigua(cidade):
        cidade_norm = _norm(cidade).replace(" ","")
        if cidade_norm[:4] in h or cidade_norm[:5] in h:
            score += 1

    if len(h) < 5:
        score -= 2

    bonus = {"site_validado": 8, "site_html": 8, "resultado_site": 3, "google_ig": 0}
    score += bonus.get(origem, 0)

    return max(score, 0)


def _verificar_instagram_completo(driver, handle: str, nome: str, cidade: str, site_url: str = None) -> dict:
    url = f"https://www.instagram.com/{handle}/"
    resultado = {
        "aprovado": False,
        "motivo": None,
        "seguidores": None,
        "num_posts": None,
        "ultimo_post": None,
        "semanas": None,
        "reciproco": False,
    }

    try:
        driver.get(url)
        time.sleep(3)
        html = driver.page_source

        if ("sorry, this page" in html.lower() or
                "página não disponível" in html.lower() or
                driver.current_url == "https://www.instagram.com/"):
            resultado["motivo"] = "perfil não existe"
            return resultado

        if ("this account is private" in html.lower() or
                "esta conta é privada" in html.lower()):
            resultado["motivo"] = "perfil privado"
            return resultado

        nome_display = _extrair_nome_display_ig(html)
        if nome_display and not _nomes_compatíveis(nome, nome_display):
            resultado["motivo"] = f"nome do perfil incompatível: '{nome_display}' ≠ '{nome}'"
            return resultado

        if site_url:
            dom_site = _dominio_simples(site_url)
            if dom_site and dom_site in html.lower():
                resultado["reciproco"] = True

        cidade_perfil = _extrair_cidade_do_perfil_ig(html)
        if cidade_perfil and _cidade_conflito(cidade_perfil, cidade):
            resultado["motivo"] = f"cidade incorreta no perfil: {cidade_perfil} ≠ {cidade}"
            return resultado

        meta_m = re.search(
            r'<meta[^>]+name=["\\\']description["\\\'][^>]+content=["\\\']([^"\\\']+)["\\\']',
            html, re.IGNORECASE)
        if meta_m:
            desc = meta_m.group(1)
            seg_m = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)', desc, re.IGNORECASE)
            pos_m = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)', desc, re.IGNORECASE)
            if seg_m:
                resultado["seguidores"] = _parse_numero(seg_m.group(1))
            if pos_m:
                resultado["num_posts"] = _parse_numero(pos_m.group(1))

        if resultado["seguidores"] is None:
            for pat in [r'"edge_followed_by":\{"count":(\d+)\}', r'"follower_count":(\d+)', r'"followers":(\d+)']:
                m = re.search(pat, html)
                if m:
                    resultado["seguidores"] = int(m.group(1))
                    break

        if resultado["num_posts"] is None:
            for pat in [r'"edge_owner_to_timeline_media":\{"count":(\d+)', r'"media_count":(\d+)']:
                m = re.search(pat, html)
                if m:
                    resultado["num_posts"] = int(m.group(1))
                    break

        if resultado["seguidores"] is None or resultado["num_posts"] is None:
            try:
                from selenium.webdriver.common.by import By
                stat_links = driver.find_elements(By.CSS_SELECTOR, "ul li, section ul li")
                for li in stat_links[:6]:
                    texto_li = (li.text or "").lower().strip()
                    m_num = re.search(r'([\d\.,]+)', texto_li)
                    if not m_num:
                        continue
                    num = _parse_numero(m_num.group(1))
                    if num is None:
                        continue
                    if any(p in texto_li for p in ["publicaç","post","publicacoes"]):
                        if resultado["num_posts"] is None:
                            resultado["num_posts"] = num
                    elif any(p in texto_li for p in ["seguidor","follower"]):
                        if resultado["seguidores"] is None:
                            resultado["seguidores"] = num
            except Exception:
                pass

        seg = resultado["seguidores"]
        posts = resultado["num_posts"]

        if seg is not None and seg < MIN_SEGUIDORES:
            resultado["motivo"] = f"poucos seguidores ({seg} < {MIN_SEGUIDORES})"
            return resultado

        if posts is not None and posts < MIN_POSTS:
            resultado["motivo"] = f"poucos posts ({posts} < {MIN_POSTS})"
            return resultado

        data_ultimo = _extrair_data_ultimo_post_nao_fixado(driver, html)
        resultado["ultimo_post"] = data_ultimo

        if data_ultimo:
            try:
                dt = date.fromisoformat(data_ultimo)
                semanas = (date.today() - dt).days // 7
                resultado["semanas"] = semanas
                if semanas > MAX_SEMANAS_INATIVO:
                    resultado["motivo"] = f"inativo — último post há {semanas} semanas (máx {MAX_SEMANAS_INATIVO})"
                    return resultado
            except Exception:
                pass
        else:
            resultado["motivo"] = "data do último post não detectada — revisar manualmente"
            resultado["aprovado"] = True
            return resultado

        html_l = html.lower()
        palavras = _palavras(nome)
        hits_nome = sum(1 for p in palavras if p in html_l)
        if hits_nome < 1:
            resultado["motivo"] = "nome da empresa não encontrado no perfil"
            return resultado

        resultado["aprovado"] = True
        return resultado

    except Exception as e:
        resultado["motivo"] = f"erro selenium: {str(e)[:80]}"
        return resultado


def _extrair_data_ultimo_post_nao_fixado(driver, html: str) -> str | None:
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        time.sleep(1.5)

        posts = driver.find_elements(By.CSS_SELECTOR, "article a[href*='/p/'], a[href*='/p/']")
        hrefs, vistos = [], set()
        for p in posts:
            href = p.get_attribute("href") or ""
            if "/p/" in href and href not in vistos:
                vistos.add(href)
                hrefs.append(href)
            if len(hrefs) >= 4:
                break

        if not hrefs:
            return None

        url_perfil = driver.current_url
        datas = []

        for idx, href in enumerate(hrefs):
            try:
                driver.get(href)
                time.sleep(2)
                try:
                    tel = WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "time[datetime]"))
                    )
                    dt = tel.get_attribute("datetime")
                    if dt:
                        datas.append((idx, dt[:10]))
                        continue
                except Exception:
                    pass
                ph = driver.page_source
                m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', ph)
                if m:
                    datas.append((idx, m.group(1)))
            except Exception:
                continue

        driver.get(url_perfil)
        time.sleep(1.5)

        if not datas:
            return None
        if len(datas) == 1:
            return datas[0][1]

        data_post1 = datas[0][1]
        datas_outros = [d for i, d in datas if i > 0]
        mais_recente = max(datas_outros) if datas_outros else None

        return mais_recente if (mais_recente and mais_recente > data_post1) else data_post1

    except Exception:
        return None


def _parse_numero(s: str) -> int | None:
    try:
        s = s.strip()
        tem_mil = "mil" in s.lower()
        tem_k = s.lower().endswith("k") and not tem_mil
        s_num = re.sub(r"(?:mil|k)", "", s, flags=re.IGNORECASE).strip()
        if tem_mil or tem_k:
            s_num = s_num.replace(".", "").replace(",", ".")
            try:
                valor = float(s_num)
            except ValueError:
                m = re.search(r"\d+", s_num)
                valor = float(m.group()) if m else 0.0
            return int(round(valor * 1000))
        return int(s_num.replace(".", "").replace(",", ""))
    except Exception:
        return None


def _extrair_ig_do_site(site_url: str, nome: str, cidade: str) -> str | None:
    try:
        r = requests.get(site_url, headers=HEADERS, timeout=TIMEOUT_PAGINA)
        if r.status_code != 200:
            return None
        handles = []
        for m in re.finditer(r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', r.text):
            h = m.group(1).lower()
            if h not in IGNORAR_IG and h not in handles:
                handles.append(h)
        if handles:
            return max(handles, key=lambda h: _pontuar_ig(h, nome, cidade, "site_html"))
    except Exception:
        pass
    return None


def _descobrir_site(driver, nome: str, cidade: str, log) -> dict:
    query = f"{nome} {_cidade_query(cidade)} imobiliária"
    log(f"  🌐 Buscando site no DuckDuckGo: {query}")
    urls = _buscar_duckduckgo(driver, query)

    candidatos_site = []
    handles_ig = []

    for url in urls:
        score = _pontuar_site(url, nome, cidade)
        if score == -1:
            m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', url)
            if m:
                h = m.group(1).lower()
                if h not in IGNORAR_IG:
                    handles_ig.append(h)
                    log(f"    📸 IG nos resultados: @{h}")
            continue
        candidatos_site.append({"url": url, "score": score})

    aprovados = sorted([c for c in candidatos_site if c["score"] > 0], key=lambda x: -x["score"])

    if not aprovados:
        log("    → sem candidatos de site com score > 0")
        return {
            "url": None,
            "sem_site": True,
            "ig_dos_resultados": handles_ig,
            "ig_do_site_validado": None,
            "site_score": 0,
            "site_sinais": [],
        }

    for cand in aprovados[:MAX_TENTATIVAS_SITE]:
        url = cand["url"]
        log(f"    → verificando: {url[:55]} (score_url={cand['score']})")
        v = _verificar_site(url, nome, cidade)
        log(f"    {'✅' if v['ok'] else '❌'} {v['motivo']}")

        if v["ok"]:
            ig_validado = v.get("ig_link")
            if ig_validado:
                log(f"    🔗 IG encontrado no site: @{ig_validado}")
            return {
                "url": url,
                "sem_site": False,
                "ig_dos_resultados": handles_ig,
                "ig_do_site_validado": ig_validado,
                "site_score": v["score"],
                "site_sinais": v["sinais_fortes"],
            }

    return {
        "url": None,
        "sem_site": True,
        "ig_dos_resultados": handles_ig,
        "ig_do_site_validado": None,
        "site_score": 0,
        "site_sinais": [],
    }


def _descobrir_instagram(driver, nome: str, cidade: str, site_url: str,
                         ig_do_site_validado: str, ig_dos_resultados: list, log) -> dict:
    log("  📸 Buscando Instagram...")

    candidatos = []
    vistos = set()

    if ig_do_site_validado and ig_do_site_validado not in vistos:
        vistos.add(ig_do_site_validado)
        score = _pontuar_ig(ig_do_site_validado, nome, cidade, "site_validado")
        candidatos.append({"handle": ig_do_site_validado, "score": score, "origem": "site_validado"})
        log(f"    🥇 Candidato do site validado: @{ig_do_site_validado} (score={score})")

    if site_url:
        h = _extrair_ig_do_site(site_url, nome, cidade)
        if h and h not in vistos:
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade, "site_html")
            candidatos.append({"handle": h, "score": score, "origem": "site_html"})
            log(f"    🥈 Candidato do HTML do site: @{h} (score={score})")

    for h in ig_dos_resultados:
        if h not in vistos:
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade, "resultado_site")
            candidatos.append({"handle": h, "score": score, "origem": "resultado_site"})

    tem_origem_confiavel = any(c["origem"] in ("site_validado", "site_html") for c in candidatos)

    if not tem_origem_confiavel:
        query = f"{nome} {_cidade_query(cidade)} site:instagram.com"
        urls = _buscar_duckduckgo(driver, query, n=10)
        for url in urls:
            m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', url)
            if not m:
                continue
            h = m.group(1).lower()
            if h in IGNORAR_IG or h in vistos:
                continue
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade, "google_ig")
            candidatos.append({"handle": h, "score": score, "origem": "google_ig"})
            log(f"    🔍 Candidato DuckDuckGo: @{h} (score={score})")

    aprovados = sorted([c for c in candidatos if c["score"] > 0], key=lambda x: -x["score"])

    if not aprovados:
        log("    → sem candidatos de Instagram com score > 0")
        return {"url": None, "handle": None, "sem_ig": True, "metodo": None, "origem": None, "motivo": "sem candidatos"}

    for cand in aprovados[:MAX_TENTATIVAS_IG]:
        h = cand["handle"]
        origem = cand["origem"]
        log(f"    → verificando @{h} (score={cand['score']}, origem={origem})")

        v = _verificar_instagram_completo(driver, h, nome, cidade, site_url=site_url)

        seg_s = f" | seg={v['seguidores']}" if v.get("seguidores") else ""
        pos_s = f" | posts={v['num_posts']}" if v.get("num_posts") else ""
        dat_s = f" | último={v['ultimo_post']}" if v.get("ultimo_post") else ""
        rec_s = " | ♻️ RECÍPROCO" if v.get("reciproco") else ""
        log(f"    {'✅' if v['aprovado'] else '❌'} {v['motivo'] or 'ok'}{seg_s}{pos_s}{dat_s}{rec_s}")

        if not v["aprovado"]:
            continue

        reciproco = v.get("reciproco", False)

        if origem == "google_ig" and not reciproco:
            log("    ⛔ Rejeitado — origem google_ig sem reciprocidade (bio não aponta para o site)")
            continue

        return {
            "url": f"https://www.instagram.com/{h}/",
            "handle": h,
            "sem_ig": False,
            "metodo": origem,
            "origem": origem,
            "reciproco": reciproco,
            "seguidores": v.get("seguidores"),
            "num_posts": v.get("num_posts"),
            "ultimo_post": v.get("ultimo_post"),
            "semanas": v.get("semanas"),
            "motivo": v.get("motivo"),
        }

    return {"url": None, "handle": None, "sem_ig": True, "metodo": None, "origem": None,
            "motivo": aprovados[0].get("motivo") if aprovados else None}


def enriquecer(driver, nome: str, cidade: str, log=None) -> ResultadoEnriquecimento:
    def _log(msg):
        if log:
            log(msg)

    r = ResultadoEnriquecimento(nome=nome, cidade=cidade)
    try:
        site = _descobrir_site(driver, nome, cidade, _log)
        r.site_url = site["url"]
        r.site_confirmado = bool(site["url"])
        r.site_score = site.get("site_score", 0)
        r.site_sinais = site.get("site_sinais", [])
        r.sem_site = site["sem_site"]
        time.sleep(1)

        ig = _descobrir_instagram(
            driver, nome, cidade,
            site_url=r.site_url,
            ig_do_site_validado=site.get("ig_do_site_validado"),
            ig_dos_resultados=site.get("ig_dos_resultados", []),
            log=_log,
        )
        r.instagram_url = ig.get("url")
        r.ig_handle = ig.get("handle")
        r.ig_confirmado = bool(ig.get("url"))
        r.ig_metodo = ig.get("metodo")
        r.ig_origem = ig.get("origem")
        r.ig_reciproco = ig.get("reciproco", False)
        r.ig_seguidores = ig.get("seguidores")
        r.ig_num_posts = ig.get("num_posts")
        r.ig_ultimo_post = ig.get("ultimo_post")
        r.ig_semanas = ig.get("semanas")
        r.sem_instagram = ig.get("sem_ig", True)
        r.motivo_rejeicao_ig = ig.get("motivo")

        flags = []
        if r.site_url and r.site_score < 30:
            flags.append(f"site:score_baixo:{r.site_score}")
        if r.instagram_url and r.motivo_rejeicao_ig and "revisar manualmente" in (r.motivo_rejeicao_ig or ""):
            flags.append("ig:data_nao_detectada")
        if r.instagram_url and r.ig_seguidores is None:
            flags.append("ig:seguidores_nao_verificados")
        if r.instagram_url and r.ig_origem == "google_ig" and not r.ig_reciproco:
            flags.append("ig:sem_reciprocidade")
        r.review_flags = flags

    except Exception as e:
        r.erro = str(e)
        _log(f"  ⚠️ Erro: {e}")

    return r
