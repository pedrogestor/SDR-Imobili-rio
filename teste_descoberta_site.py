"""
teste_descoberta_site.py v2 — Valida descoberta de site via DuckDuckGo.
Roda no CMD: python teste_descoberta_site.py
"""

import re, time, sys, traceback, requests, random
from urllib.parse import urlparse, quote
from selenium.webdriver.common.by import By

EMPRESAS = [
    {"nome": "Lopes Consultoria de Imóveis", "cidade": "São Paulo",  "site_esperado": "lopes.com.br"},
    {"nome": "In Home Imoveis",              "cidade": "Goiania",    "site_esperado": "inhomeimoveis.com.br"},
    {"nome": "F. Veiga Imóveis",             "cidade": "Goiania",    "site_esperado": None},
]

PORTAIS = [
    "zapimoveis","vivareal","olx","quintoandar","chavesnamao","imovelweb",
    "wimoveis","netimoveis","123imoveis","facebook","instagram","linkedin",
    "twitter","google","bing","gstatic","duckduckgo","cnpj.biz","cnpja",
    "receitaws","minhareceita","econodata","jusbrasil","escavador","solutudo",
    "procuroacho","advdinamico","serasaexperian","serasa","reclameaqui",
    "guiamais","todosnegocios","brasillocais","getninjas","reddit","apple",
    "saopauloguiaonline","avaliacoesbrasil","eguias",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
           "Accept-Language": "pt-BR,pt;q=0.9"}

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
    if not any(dominio.endswith(e) for e in exts): return 0, "extensão incomum"
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
    url = f"https://duckduckgo.com/?q={quote(query)}&kl=br-pt"
    driver.get(url)
    time.sleep(4 + random.uniform(0, 1.5))
    html = driver.page_source
    if len(html) < 5000 or "captcha" in html.lower():
        return [], "bloqueado"
    urls, vistos = [], set()
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "article h2 a")
        for el in els:
            href = el.get_attribute("href") or ""
            if not href or "duckduckgo.com" in href: continue
            d = urlparse(href).netloc
            if d and d not in vistos:
                vistos.add(d); urls.append(href)
    except Exception:
        pass
    if not urls:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, "[data-testid='result-title-a']")
            for el in els:
                href = el.get_attribute("href") or ""
                if not href or "duckduckgo.com" in href: continue
                d = urlparse(href).netloc
                if d and d not in vistos:
                    vistos.add(d); urls.append(href)
        except Exception:
            pass
    return urls[:n], "ok"

if __name__ == "__main__":
    print("\n🦆 Teste descoberta de site — DuckDuckGo DOM\n")

    try:
        from selenium import webdriver
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable,"-m","pip","install","selenium","webdriver-manager"])

    driver = criar_driver()
    print("Chrome OK ✅\n")

    resultados = []
    try:
        for emp in EMPRESAS:
            print(f"\n{'='*60}\n  {emp['nome']} | {emp['cidade']}\n{'='*60}")
            query = f"{emp['nome']} {emp['cidade']} imobiliária"
            print(f"Query: {query}")

            urls, status = buscar_duckduckgo(driver, query)
            print(f"Status: {status} | URLs: {len(urls)}")

            aprovadas = []
            for url in urls:
                score, motivo = pontuar_url(url, emp["nome"], emp["cidade"])
                icone = "✅" if score > 0 else ("📸" if score==-1 else "❌")
                print(f"  {icone} score={score:2d} | {url[:65]}")
                if score > 0:
                    aprovadas.append((url, score))

            esperado = emp.get("site_esperado")
            melhor   = sorted(aprovadas, key=lambda x: -x[1])[0][0] if aprovadas else None
            achou    = esperado and any(esperado in u for u in urls)

            print(f"\n  Melhor candidato: {melhor or 'nenhum'}")
            if esperado:
                print(f"  Site esperado ({esperado}): {'✅ ENCONTRADO' if achou else '❌ NÃO encontrado'}")

            resultados.append({
                "nome": emp["nome"],
                "melhor": melhor,
                "achou_esperado": achou,
                "n_aprovadas": len(aprovadas),
            })
            time.sleep(3)

    except Exception:
        print(f"\n❌ ERRO:\n{traceback.format_exc()}")
    finally:
        driver.quit()
        print("\nChrome encerrado.")

    print(f"\n{'='*60}\nRESUMO\n{'='*60}")
    for r in resultados:
        ok = r["n_aprovadas"] > 0
        print(f"  {'✅' if ok else '❌'} {r['nome']}")
        print(f"     Melhor: {r['melhor'] or 'nenhum'}")
        if r["achou_esperado"] is not None:
            print(f"     Esperado: {'✅' if r['achou_esperado'] else '❌'}")

    input("\nENTER para fechar...")
