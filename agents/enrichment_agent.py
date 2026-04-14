"""
agents/enrichment_agent.py — Enriquecimento de site + Instagram.
Instagram: verificação completa via Selenium (seguidores, posts, último post não fixado).
Site: verificação de conteúdo via requests.
"""

import re, time, requests
from datetime import datetime, timezone, date
from urllib.parse import urlparse, quote
from dataclasses import dataclass

# ── Critérios mínimos Instagram ───────────────────────────────────────────────
MIN_SEGUIDORES      = 500
MIN_POSTS           = 20
MAX_SEMANAS_INATIVO = 8

MAX_TENTATIVAS_SITE = 5
MAX_TENTATIVAS_IG   = 5
TIMEOUT_PAGINA      = 10

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
    nome:              str
    cidade:            str
    # Site
    site_url:          str  = None
    site_confirmado:   bool = False
    sem_site:          bool = False
    # Instagram
    instagram_url:     str  = None
    ig_handle:         str  = None
    ig_confirmado:     bool = False
    ig_metodo:         str  = None
    ig_seguidores:     int  = None
    ig_num_posts:      int  = None
    ig_ultimo_post:    str  = None
    ig_semanas:        int  = None
    sem_instagram:     bool = False
    motivo_rejeicao_ig: str = None
    # Geral
    erro:              str  = None


# ── Normalização ──────────────────────────────────────────────────────────────

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


# ── Selenium ──────────────────────────────────────────────────────────────────

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
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def _buscar_google(driver, query: str, n: int = 10) -> list:
    from urllib.parse import quote as _quote
    url = (f"https://www.google.com.br/search?q={_quote(query)}"
           f"&hl=pt-BR&gl=br&num={n}")
    driver.get(url)
    time.sleep(2.5)
    html = driver.page_source

    urls, vistos = [], set()
    for u in re.findall(r'/url\?q=(https?://[^&"]{10,200})', html):
        u = u.split("&")[0]
        d = urlparse(u).netloc
        if d and d not in vistos and "google" not in d:
            vistos.add(d); urls.append(u)
    for u in re.findall(
            r'href="(https?://(?!(?:www\.)?google)[^"]{10,200})"', html):
        d = urlparse(u).netloc
        if d and d not in vistos:
            vistos.add(d); urls.append(u)
    return urls[:n]


# ── Pontuação ─────────────────────────────────────────────────────────────────

def _pontuar_site(url: str, nome: str, cidade: str) -> int:
    dominio = urlparse(url).netloc.lower().replace("www.", "")
    path    = urlparse(url).path.lower()

    if "instagram.com" in dominio:
        return -1  # sinaliza Instagram

    for p in PORTAIS:
        if p in dominio:
            return 0
    extensoes = ['.com.br','.com','.net.br','.net','.org.br','.org','.imb.br']
    if not any(dominio.endswith(e) for e in extensoes):
        return 0
    if any(x in dominio for x in ["gstatic","cloudfront","amazonaws","cdn"]):
        return 0

    score    = 0
    palavras = _palavras(nome)
    hits     = sum(1 for p in palavras if p in dominio)
    score   += hits * 3
    if _norm(cidade)[:5] in dominio:
        score += 1
    if dominio.endswith('.com.br'):
        score += 1
    if path.count('/') > 3:
        score -= 1
    return max(score, 0)


def _pontuar_ig(handle: str, nome: str, cidade: str,
                bonus: bool = False) -> int:
    if handle in IGNORAR_IG or len(handle) < 3:
        return 0
    h        = handle.lower()
    score    = 0
    palavras = _palavras(nome)
    hits     = sum(1 for p in palavras if p in h)
    score   += hits * 3
    termos = ["imob","imovel","imoveis","corretor","corretora",
              "residencial","incorpor","casas","aptos","apartamentos"]
    if any(t in h for t in termos):
        score += 2
    if _norm(cidade)[:4] in h or _norm(cidade)[:5] in h:
        score += 1
    if len(h) < 5:
        score -= 2
    if bonus:
        score += 3
    return max(score, 0)


# ── Verificação de site (requests) ───────────────────────────────────────────

def _verificar_site(url: str, nome: str, cidade: str) -> dict:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_PAGINA,
                         allow_redirects=True)
        if r.status_code >= 400:
            return {"ok": False, "motivo": f"HTTP {r.status_code}"}

        dom_final = urlparse(r.url).netloc.lower()
        for p in PORTAIS:
            if p in dom_final:
                return {"ok": False, "motivo": f"redirecionou para portal ({p})"}

        html_l    = r.text.lower()
        score     = 0
        palavras  = _palavras(nome)
        hits_nome = sum(1 for p in palavras if p in html_l)
        score    += hits_nome * 2
        if _norm(cidade) in _norm(r.text):
            score += 1
        hits_imob = sum(1 for t in TERMOS_IMOB if t in html_l)
        score    += min(hits_imob, 2)

        if hits_nome < 1:
            return {"ok": False, "motivo": f"nome ausente (score={score})"}
        if score < 4:
            return {"ok": False, "motivo": f"score insuficiente ({score})"}
        return {"ok": True, "motivo": f"score={score}"}

    except requests.Timeout:
        return {"ok": False, "motivo": "timeout"}
    except Exception as e:
        return {"ok": False, "motivo": str(e)[:60]}


# ── Verificação completa de Instagram (Selenium) ──────────────────────────────

def _verificar_instagram_completo(driver, handle: str,
                                   nome: str, cidade: str) -> dict:
    """
    Abre o perfil no Selenium. Extrai:
    - seguidores, posts, privado
    - último post não fixado e sua data
    Retorna dict completo de verificação.
    """
    url = f"https://www.instagram.com/{handle}/"
    resultado = {
        "aprovado":    False,
        "motivo":      None,
        "seguidores":  None,
        "num_posts":   None,
        "ultimo_post": None,
        "semanas":     None,
    }

    try:
        driver.get(url)
        time.sleep(3)
        html = driver.page_source

        # Perfil não existe
        if ("sorry, this page" in html.lower() or
                "página não disponível" in html.lower() or
                driver.current_url == "https://www.instagram.com/"):
            resultado["motivo"] = "perfil não existe"
            return resultado

        # Privado
        if ("this account is private" in html.lower() or
                "esta conta é privada" in html.lower()):
            resultado["motivo"] = "perfil privado"
            return resultado

        # ── Extrai seguidores e posts do HTML ─────────────────────────────
        # Método 1: meta description "X seguidores, Y seguindo, Z publicações"
        meta_desc = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            html, re.IGNORECASE)
        if meta_desc:
            desc = meta_desc.group(1)
            seg_m = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:seguidores|followers)', desc, re.IGNORECASE)
            pos_m = re.search(r'([\d,\.]+\s*(?:mil|k)?)\s*(?:publicações|posts)', desc, re.IGNORECASE)
            if seg_m:
                resultado["seguidores"] = _parse_numero(seg_m.group(1))
            if pos_m:
                resultado["num_posts"] = _parse_numero(pos_m.group(1))

        # Método 2: JSON embutido
        if resultado["seguidores"] is None:
            for padrao in [
                r'"edge_followed_by":\{"count":(\d+)\}',
                r'"follower_count":(\d+)',
                r'"followers":(\d+)',
            ]:
                m = re.search(padrao, html)
                if m:
                    resultado["seguidores"] = int(m.group(1))
                    break

        if resultado["num_posts"] is None:
            for padrao in [
                r'"edge_owner_to_timeline_media":\{"count":(\d+)',
                r'"media_count":(\d+)',
            ]:
                m = re.search(padrao, html)
                if m:
                    resultado["num_posts"] = int(m.group(1))
                    break

        # Método 3: contagem visível na página via Selenium
        # Lê os <li> da barra de stats e identifica cada um pelo label
        if resultado["seguidores"] is None or resultado["num_posts"] is None:
            try:
                from selenium.webdriver.common.by import By

                # Tenta extrair via aria-label dos links de stats
                # Formato Instagram: "X publicações", "X seguidores", "Y seguindo"
                stat_links = driver.find_elements(
                    By.CSS_SELECTOR, "ul li, section ul li")

                for li in stat_links[:6]:
                    texto_li = (li.text or "").lower().strip()
                    # Extrai o número do texto do li
                    m_num = re.search(r'([\d\.,]+)', texto_li)
                    if not m_num:
                        continue
                    num = _parse_numero(m_num.group(1))
                    if num is None:
                        continue

                    if any(p in texto_li for p in
                           ["publicaç", "post", "publicacoes"]):
                        if resultado["num_posts"] is None:
                            resultado["num_posts"] = num

                    elif any(p in texto_li for p in
                             ["seguidor", "follower"]):
                        if resultado["seguidores"] is None:
                            resultado["seguidores"] = num

                # Fallback final: spans numéricos com heurística de posição
                # Só usa se ainda não conseguiu pelo método acima
                if resultado["seguidores"] is None or resultado["num_posts"] is None:
                    spans = driver.find_elements(
                        By.CSS_SELECTOR,
                        "header section ul li span[class*='x5n08af'], "
                        "header section ul li span span"
                    )
                    numeros_com_contexto = []
                    for span in spans[:9]:
                        txt = (span.text or "").strip()
                        # Remove separadores de milhar e ponto decimal BR
                        txt_limpo = txt.replace(".", "").replace(",", "")
                        if txt_limpo.isdigit():
                            # Pega o texto do li pai para identificar contexto
                            try:
                                li_pai = driver.execute_script(
                                    "return arguments[0].closest('li')", span)
                                contexto = (li_pai.text or "").lower() if li_pai else ""
                            except Exception:
                                contexto = ""
                            numeros_com_contexto.append(
                                (int(txt_limpo), contexto))

                    for num, ctx in numeros_com_contexto:
                        if "publicaç" in ctx or "post" in ctx:
                            if resultado["num_posts"] is None:
                                resultado["num_posts"] = num
                        elif "seguidor" in ctx or "follower" in ctx:
                            if resultado["seguidores"] is None:
                                resultado["seguidores"] = num

            except Exception:
                pass

        # Verifica critérios de seguidores e posts
        seg = resultado["seguidores"]
        posts = resultado["num_posts"]

        if seg is not None and seg < MIN_SEGUIDORES:
            resultado["motivo"] = f"poucos seguidores ({seg} < {MIN_SEGUIDORES})"
            return resultado

        if posts is not None and posts < MIN_POSTS:
            resultado["motivo"] = f"poucos posts ({posts} < {MIN_POSTS})"
            return resultado

        # ── Último post não fixado ─────────────────────────────────────────
        data_ultimo = _extrair_data_ultimo_post_nao_fixado(driver, html)
        resultado["ultimo_post"] = data_ultimo

        if data_ultimo:
            try:
                dt = date.fromisoformat(data_ultimo)
                semanas = (date.today() - dt).days // 7
                resultado["semanas"] = semanas
                if semanas > MAX_SEMANAS_INATIVO:
                    resultado["motivo"] = (
                        f"inativo — último post há {semanas} semanas "
                        f"(máx {MAX_SEMANAS_INATIVO})")
                    return resultado
            except Exception:
                pass
        else:
            # Não conseguiu detectar data — marca para revisão manual
            resultado["motivo"] = "data do último post não detectada — revisar manualmente"
            # Não reprova automaticamente, aprova com ressalva
            resultado["aprovado"] = True
            return resultado

        # Verificação de conteúdo: nome ou termos imobiliários na página
        html_l    = html.lower()
        palavras  = _palavras(nome)
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
    """
    Detecta post fixado por comparação de datas entre os 4 primeiros posts.

    Lógica:
    1. Abre os 4 primeiros posts do grid e coleta suas datas
    2. Se algum dos posts 2, 3 ou 4 tiver data MAIS RECENTE que o post 1,
       então o post 1 é fixado — usa a data mais recente encontrada
    3. Se o post 1 for o mais recente, ele é o último post real
    4. Retorna a data do último post não fixado
    """
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        time.sleep(1.5)

        # Coleta links dos 4 primeiros posts do grid
        posts = driver.find_elements(
            By.CSS_SELECTOR,
            "article a[href*='/p/'], a[href*='/p/']"
        )
        hrefs = []
        vistos = set()
        for p in posts:
            href = p.get_attribute("href") or ""
            if "/p/" in href and href not in vistos:
                vistos.add(href)
                hrefs.append(href)
            if len(hrefs) >= 4:
                break

        if not hrefs:
            return None

        # Abre cada post e extrai a data
        url_perfil = driver.current_url
        datas: list[tuple[int, str]] = []  # (index, data_iso)

        for idx, href in enumerate(hrefs):
            try:
                driver.get(href)
                time.sleep(2)

                # Tenta <time datetime="...">
                try:
                    tel = WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "time[datetime]"))
                    )
                    dt = tel.get_attribute("datetime")
                    if dt:
                        datas.append((idx, dt[:10]))
                        continue
                except Exception:
                    pass

                # Fallback: regex no HTML
                ph = driver.page_source
                m  = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', ph)
                if m:
                    datas.append((idx, m.group(1)))

            except Exception:
                continue

        # Volta ao perfil
        driver.get(url_perfil)
        time.sleep(1.5)

        if not datas:
            return None

        if len(datas) == 1:
            return datas[0][1]

        # Compara datas para detectar post fixado
        data_post1 = datas[0][1]
        datas_outros = [d for i, d in datas if i > 0]

        # Se algum dos posts 2-4 for mais recente que o post 1 → post 1 é fixado
        mais_recente_outros = max(datas_outros) if datas_outros else None

        if mais_recente_outros and mais_recente_outros > data_post1:
            # Post 1 é fixado — retorna a data mais recente dos demais
            return mais_recente_outros
        else:
            # Post 1 é o mais recente — é o último post real
            return data_post1

    except Exception:
        return None


def _parse_numero(s: str) -> int | None:
    """
    Converte strings de contagem do Instagram para inteiro.
    Exemplos: "18,1 mil" -> 18100, "1.200" -> 1200, "5k" -> 5000, "181" -> 181
    Regra BR: vírgula = separador decimal, ponto = separador de milhar.
    """
    try:
        s = s.strip()
        tem_mil = "mil" in s.lower()
        tem_k   = s.lower().endswith("k") and not tem_mil

        # Remove sufixos
        s_num = re.sub(r"(?:mil|k)", "", s, flags=re.IGNORECASE).strip()

        if tem_mil or tem_k:
            # Número pode ter decimal: "18,1" = 18.1 no padrão BR
            # Troca vírgula por ponto para float
            s_num = s_num.replace(".", "").replace(",", ".")
            try:
                valor = float(s_num)
            except ValueError:
                # Tenta só a parte inteira
                m = re.search(r"\d+", s_num)
                valor = float(m.group()) if m else 0.0
            return int(round(valor * 1000))
        else:
            # Sem sufixo: ponto = milhar, vírgula = decimal (ou separador de milhar)
            # Remove ambos e converte direto
            s_num = s_num.replace(".", "").replace(",", "")
            return int(s_num)
    except Exception:
        return None


def _extrair_ig_do_site(site_url: str, nome: str, cidade: str) -> str | None:
    try:
        r = requests.get(site_url, headers=HEADERS, timeout=TIMEOUT_PAGINA)
        if r.status_code != 200:
            return None
        handles = []
        for m in re.finditer(
                r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', r.text):
            h = m.group(1).lower()
            if h not in IGNORAR_IG and h not in handles:
                handles.append(h)
        if handles:
            return max(handles, key=lambda h: _pontuar_ig(h, nome, cidade))
    except Exception:
        pass
    return None


# ── Descoberta de site ────────────────────────────────────────────────────────

def _descobrir_site(driver, nome: str, cidade: str, log) -> dict:
    log(f"  🌐 Buscando site...")
    urls = _buscar_google(driver, f"{nome} {cidade} imobiliária")

    candidatos_site = []
    handles_ig      = []

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

    aprovados = sorted([c for c in candidatos_site if c["score"] > 0],
                       key=lambda x: -x["score"])

    if not aprovados:
        log("    → sem candidatos de site")
        return {"url": None, "sem_site": True, "ig_dos_resultados": handles_ig}

    for cand in aprovados[:MAX_TENTATIVAS_SITE]:
        url = cand["url"]
        log(f"    → verificando: {url[:55]} (score={cand['score']})")
        v = _verificar_site(url, nome, cidade)
        if v["ok"]:
            log(f"    ✅ site confirmado")
            return {"url": url, "sem_site": False,
                    "ig_dos_resultados": handles_ig}
        log(f"    ❌ {v['motivo']}")

    return {"url": None, "sem_site": True, "ig_dos_resultados": handles_ig}


# ── Descoberta de Instagram ───────────────────────────────────────────────────

def _descobrir_instagram(driver, nome: str, cidade: str,
                          site_url: str, ig_dos_resultados: list,
                          log) -> dict:
    log(f"  📸 Buscando Instagram...")

    candidatos = []
    vistos     = set()

    # Prioridade 1: capturados dos resultados de site
    for h in ig_dos_resultados:
        if h not in vistos:
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade, bonus=True)
            candidatos.append({"handle": h, "score": score,
                                "origem": "resultado_site"})

    # Prioridade 2: extraído do HTML do site
    if site_url:
        h = _extrair_ig_do_site(site_url, nome, cidade)
        if h and h not in vistos:
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade) + 2
            candidatos.append({"handle": h, "score": score,
                                "origem": "site_html"})

    # Prioridade 3: busca direta Google
    if len(candidatos) < 3:
        urls = _buscar_google(driver,
                              f"{nome} {cidade} site:instagram.com", n=10)
        for url in urls:
            m = re.search(r'instagram\.com/([a-zA-Z0-9_\.]{3,30})/?', url)
            if not m:
                continue
            h = m.group(1).lower()
            if h in IGNORAR_IG or h in vistos:
                continue
            vistos.add(h)
            score = _pontuar_ig(h, nome, cidade)
            candidatos.append({"handle": h, "score": score,
                                "origem": "google_ig"})

    aprovados = sorted([c for c in candidatos if c["score"] > 0],
                       key=lambda x: -x["score"])

    if not aprovados:
        log("    → sem candidatos de Instagram")
        return {"url": None, "handle": None, "sem_ig": True,
                "motivo": "sem candidatos"}

    for cand in aprovados[:MAX_TENTATIVAS_IG]:
        h = cand["handle"]
        log(f"    → verificando @{h} (score={cand['score']}, "
            f"origem={cand['origem']})")

        v = _verificar_instagram_completo(driver, h, nome, cidade)

        seg_str  = f" | seg={v['seguidores']}" if v.get("seguidores") else ""
        post_str = f" | posts={v['num_posts']}" if v.get("num_posts") else ""
        data_str = f" | último={v['ultimo_post']}" if v.get("ultimo_post") else ""
        log(f"    {'✅' if v['aprovado'] else '❌'} {v['motivo'] or 'ok'}"
            f"{seg_str}{post_str}{data_str}")

        if v["aprovado"]:
            return {
                "url":        f"https://www.instagram.com/{h}/",
                "handle":     h,
                "sem_ig":     False,
                "metodo":     cand["origem"],
                "seguidores": v.get("seguidores"),
                "num_posts":  v.get("num_posts"),
                "ultimo_post": v.get("ultimo_post"),
                "semanas":    v.get("semanas"),
                "motivo":     v.get("motivo"),
            }

    return {"url": None, "handle": None, "sem_ig": True,
            "metodo": None, "motivo": aprovados[0].get("motivo") if aprovados else None}


# ── Orquestrador ──────────────────────────────────────────────────────────────

def enriquecer(driver, nome: str, cidade: str,
               log=None) -> ResultadoEnriquecimento:
    def _log(msg):
        if log:
            log(msg)

    r = ResultadoEnriquecimento(nome=nome, cidade=cidade)
    try:
        site = _descobrir_site(driver, nome, cidade, _log)
        r.site_url      = site["url"]
        r.site_confirmado = bool(site["url"])
        r.sem_site      = site["sem_site"]
        time.sleep(1)

        ig = _descobrir_instagram(
            driver, nome, cidade,
            site_url=r.site_url,
            ig_dos_resultados=site.get("ig_dos_resultados", []),
            log=_log,
        )
        r.instagram_url    = ig.get("url")
        r.ig_handle        = ig.get("handle")
        r.ig_confirmado    = bool(ig.get("url"))
        r.ig_metodo        = ig.get("metodo")
        r.ig_seguidores    = ig.get("seguidores")
        r.ig_num_posts     = ig.get("num_posts")
        r.ig_ultimo_post   = ig.get("ultimo_post")
        r.ig_semanas       = ig.get("semanas")
        r.sem_instagram    = ig.get("sem_ig", True)
        r.motivo_rejeicao_ig = ig.get("motivo")

    except Exception as e:
        r.erro = str(e)
        _log(f"  ⚠️ Erro: {e}")

    return r
