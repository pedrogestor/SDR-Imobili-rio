"""
teste_ddg_debug.py v2 — DuckDuckGo é React SPA.
Usa Selenium find_elements para pegar links do DOM renderizado.
"""

import re, time, sys
from urllib.parse import quote, urlparse

PORTAIS = [
    "zapimoveis","vivareal","olx","quintoandar","chavesnamao","imovelweb",
    "wimoveis","netimoveis","123imoveis","facebook","instagram","linkedin",
    "twitter","google","bing","gstatic","duckduckgo","cnpj.biz","cnpja",
    "receitaws","minhareceita","econodata","jusbrasil","escavador","solutudo",
    "procuroacho","advdinamico","serasaexperian","serasa","reclameaqui",
    "guiamais","todosnegocios","brasillocais","getninjas","reddit","apple",
]

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
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

if __name__ == "__main__":
    print("Chrome iniciando...")
    driver = criar_driver()
    print("Chrome OK\n")

    query = "Lopes Consultoria de Imóveis São Paulo imobiliária"
    url   = f"https://duckduckgo.com/?q={quote(query)}&kl=br-pt"
    driver.get(url)

    # Aguarda o React renderizar os resultados
    print("Aguardando resultados renderizarem (6s)...")
    time.sleep(6)

    from selenium.webdriver.common.by import By

    # ── Testa vários seletores CSS possíveis ─────────────────────────────
    seletores = [
        "article h2 a",
        "article a[href]",
        "[data-testid='result'] a",
        "[data-testid='result-title-a']",
        "h2 a[href]",
        ".result__a",
        ".result__title a",
        "li[data-layout] a[href]",
        "ol li a[href]",
        "section a[href]",
        "a[data-testid]",
    ]

    print("\n--- Seletores CSS testados no DOM ---")
    melhor_seletor = None
    melhor_count   = 0

    for sel in seletores:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            hrefs = [e.get_attribute("href") or "" for e in els
                     if e.get_attribute("href") and
                     "duckduckgo.com" not in (e.get_attribute("href") or "")]
            if hrefs:
                print(f"  ✅ {sel:45s} → {len(hrefs)} links")
                for h in hrefs[:3]:
                    print(f"       {h[:80]}")
                if len(hrefs) > melhor_count:
                    melhor_count   = len(hrefs)
                    melhor_seletor = sel
            else:
                print(f"  ❌ {sel:45s} → 0 links")
        except Exception as e:
            print(f"  ❌ {sel:45s} → erro: {e}")

    print(f"\n--- Melhor seletor: {melhor_seletor} ({melhor_count} links) ---")

    # ── Extrai todos os links com o melhor seletor ────────────────────────
    if melhor_seletor:
        els   = driver.find_elements(By.CSS_SELECTOR, melhor_seletor)
        todos = []
        visto = set()
        for e in els:
            href = e.get_attribute("href") or ""
            if not href or "duckduckgo.com" in href:
                continue
            d = urlparse(href).netloc.lower().replace("www.", "")
            if d and d not in visto:
                visto.add(d)
                todos.append(href)

        print(f"\nLinks únicos encontrados: {len(todos)}")
        for u in todos:
            d = urlparse(u).netloc.lower().replace("www.", "")
            eh_portal = any(p in d for p in PORTAIS)
            icone = "❌" if eh_portal else "✅"
            print(f"  {icone} {u[:80]}")

        achou_lopes = any("lopes.com.br" in u for u in todos)
        print(f"\n  lopes.com.br encontrado: {'✅ SIM' if achou_lopes else '❌ NÃO'}")

    # ── Screenshot para ver o que o Chrome está mostrando ────────────────
    driver.save_screenshot("debug_ddg_renderizado.png")
    print("\nScreenshot salvo: debug_ddg_renderizado.png")

    driver.quit()
    print("Chrome encerrado.")
    input("\nENTER para fechar...")
