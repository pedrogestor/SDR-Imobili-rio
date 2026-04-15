"""
teste_duckduckgo.py — Testa busca de site via DuckDuckGo.
Compara resultado com Google para as mesmas empresas.
Roda no CMD: python teste_duckduckgo.py
"""

import re, time, sys, traceback, requests
from urllib.parse import urlparse, quote

EMPRESAS = [
    {"nome": "Lopes Consultoria de Imóveis", "cidade": "São Paulo",  "site_esperado": "lopes.com.br"},
    {"nome": "In Home Imoveis",              "cidade": "Goiania",    "site_esperado": "inhomeimoveis.com.br"},
    {"nome": "F. Veiga Imóveis",             "cidade": "Goiania",    "site_esperado": None},
]

PORTAIS = [
    "zapimoveis","vivareal","olx","quintoandar","chavesnamao",
    "imovelweb","wimoveis","netimoveis","123imoveis",
    "facebook","instagram","linkedin","twitter",
    "google","bing","gstatic","duckduckgo",
    "cnpj.biz","cnpja","receitaws","minhareceita","econodata",
    "jusbrasil","escavador","solutudo","procuroacho","advdinamico",
    "serasaexperian","serasa","reclameaqui","bing.com","yahoo",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

def sep(t, c="="): print(f"\n{c*60}\n  {t}\n{c*60}")

def _norm(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in s if not unicodedata.combining(c))

def _palavras(nome):
    ignorar = {"ltda","eireli","me","sa","s/a","e","de","da","do","das","dos",
               "em","com","para","por","a","o","as","os","imobiliaria","imoveis",
               "imovel","corretora","corretor","consultoria","negocios","servicos"}
    return [p for p in re.split(r'\W+', _norm(nome)) if len(p) > 1 and p not in ignorar]

def pontuar_url(url, nome, cidade):
    dominio = urlparse(url).netloc.lower().replace("www.", "")
    if "instagram.com" in dominio: return -1, "Instagram"
    for p in PORTAIS:
        if p in dominio: return 0, f"portal ({p})"
    exts = ['.com.br','.com','.net.br','.net','.org.br','.org','.imb.br']
    if not any(dominio.endswith(e) for e in exts): return 0, f"extensão incomum"
    score = 0
    palavras = _palavras(nome)
    hits = sum(1 for p in palavras if p in dominio)
    score += hits * 3
    if dominio.endswith('.com.br'): score += 1
    return max(score, 0), f"{hits} palavra(s) do nome no domínio"

def criar_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    opts = Options()
    # VISÍVEL — sem headless para evitar detecção
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

def buscar_duckduckgo(driver, query, n=10):
    """Busca no DuckDuckGo e extrai URLs dos resultados."""
    url = f"https://duckduckgo.com/?q={quote(query)}&kl=br-pt&t=h_"
    driver.get(url)
    time.sleep(3)
    html = driver.page_source

    # Verifica bloqueio
    if len(html) < 3000:
        return [], "bloqueado_pequeno"
    if "captcha" in html.lower():
        return [], "captcha"

    urls, vistos = [], set()

    # DuckDuckGo usa links com //duckduckgo.com/l/?uddg=URL_ENCODED
    for u in re.findall(r'uddg=(https?[^&"]+)', html):
        try:
            from urllib.parse import unquote
            u = unquote(u)
        except Exception:
            pass
        d = urlparse(u).netloc
        if d and d not in vistos and "duckduckgo" not in d:
            vistos.add(d); urls.append(u)

    # Fallback: href direto
    for u in re.findall(r'href="(https?://(?!.*duckduckgo)[^"]{10,200})"', html):
        d = urlparse(u).netloc
        if d and d not in vistos:
            vistos.add(d); urls.append(u)

    return urls[:n], "ok"

def buscar_duckduckgo_requests(query, n=10):
    """Tenta via requests simples (sem Selenium) — mais rápido."""
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}&kl=br-pt"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return [], f"http_{r.status_code}"
        html = r.text
        if len(html) < 3000:
            return [], "bloqueado"

        urls, vistos = [], set()
        # DuckDuckGo HTML version usa /l/?uddg= ou href direto
        for u in re.findall(r'href="//duckduckgo\.com/l/\?uddg=(https?[^"&]+)"', html):
            from urllib.parse import unquote
            u = unquote(u)
            d = urlparse(u).netloc
            if d and d not in vistos:
                vistos.add(d); urls.append(u)

        for u in re.findall(r'href="(https?://(?!.*duckduckgo)[^"]{10,200})"', html):
            d = urlparse(u).netloc
            if d and d not in vistos:
                vistos.add(d); urls.append(u)

        return urls[:n], "ok"
    except Exception as e:
        return [], str(e)[:60]

if __name__ == "__main__":
    print("\n🦆 Teste DuckDuckGo — busca de site\n")

    try:
        from selenium import webdriver
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip",
                               "install", "selenium", "webdriver-manager"])

    print("Iniciando Chrome (visível)...")
    try:
        driver = criar_driver()
        print("Chrome OK ✅\n")
    except Exception:
        print(f"❌ ERRO:\n{traceback.format_exc()}")
        input("ENTER..."); sys.exit(1)

    try:
        for emp in EMPRESAS:
            sep(f"{emp['nome']} | {emp['cidade']}")
            query = f"{emp['nome']} {emp['cidade']} imobiliária"

            # ── Método A: requests simples (sem Selenium) ─────────────────
            print(f"[A] DuckDuckGo via requests (sem Selenium)")
            urls_a, status_a = buscar_duckduckgo_requests(query)
            print(f"    Status: {status_a} | URLs: {len(urls_a)}")
            for u in urls_a[:5]:
                score, motivo = pontuar_url(u, emp["nome"], emp["cidade"])
                icone = "✅" if score > 0 else "❌"
                print(f"    {icone} score={score} | {u[:65]}")

            time.sleep(1)

            # ── Método B: Selenium no DuckDuckGo ─────────────────────────
            print(f"\n[B] DuckDuckGo via Selenium")
            urls_b, status_b = buscar_duckduckgo(driver, query)
            print(f"    Status: {status_b} | URLs: {len(urls_b)}")
            for u in urls_b[:5]:
                score, motivo = pontuar_url(u, emp["nome"], emp["cidade"])
                icone = "✅" if score > 0 else "❌"
                print(f"    {icone} score={score} | {u[:65]}")

            # ── Verifica se encontrou o esperado ──────────────────────────
            esperado = emp.get("site_esperado")
            if esperado:
                todas = urls_a + urls_b
                achou = any(esperado in u for u in todas)
                print(f"\n    Site esperado ({esperado}): {'✅ ENCONTRADO' if achou else '❌ NÃO ENCONTRADO'}")

            time.sleep(2)

    except Exception:
        print(f"\n❌ ERRO:\n{traceback.format_exc()}")
    finally:
        driver.quit()
        print("\nChrome encerrado.")

    sep("RESUMO")
    print("""
Se [A] (requests) funcionou sem Selenium → ótimo, mais rápido e sem bot detection.
Se [B] (Selenium) funcionou mas [A] não → usa Selenium no DuckDuckGo.
Se ambos falharam → DuckDuckGo também está bloqueando, precisamos de outra fonte.
""")
    input("ENTER para fechar...")
