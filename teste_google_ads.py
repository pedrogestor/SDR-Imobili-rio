"""
teste_google_ads.py - versao final validada.
Busca APENAS por dominio. Usa padrao confirmado: texto com contagem de anuncios.
"""

import re, time, sys, traceback
from urllib.parse import quote

TESTES = [
    {"dominio": "viverimoveisbh.com.br",         "esperado": True},
    {"dominio": "inexistente-xyz-abc-123.com.br", "esperado": False},
]


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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def _dominio(url: str):
    from urllib.parse import urlparse
    try:
        d = urlparse(url if url.startswith("http") else "https://"+url).netloc
        return d.lower().replace("www.", "") or None
    except Exception:
        return None


def verificar_google_ads(driver, dominio: str) -> dict:
    """
    Verifica anuncios no Google Ads Transparency Center pelo dominio.
    Padrao confirmado em testes: texto "N anuncios" renderizado na pagina.
    """
    resultado = {
        "anuncia":   False,
        "ads_count": 0,
        "status":    "nao_verificado",
        "erro":      None,
    }

    if not dominio:
        resultado["status"] = "sem_dominio"
        return resultado

    url = "https://adstransparency.google.com/?region=BR&domain=" + quote(dominio)
    try:
        driver.get(url)
        time.sleep(8)
        driver.execute_script("window.scrollTo(0, 300)")
        time.sleep(1)
        html = driver.page_source

        if "captcha" in html.lower():
            resultado["status"] = "captcha"
            return resultado
        if "accounts.google.com" in driver.current_url:
            resultado["status"] = "bloqueado_login"
            return resultado

        # Padrao confirmado: "8 anuncios" ou "0 anuncios" no HTML renderizado
        # Busca tanto com acento quanto sem (versao em portugues e ingles)
        padrao_pt = re.compile(r'(\d+)\s+an\u00FAncio', re.IGNORECASE)
        padrao_en = re.compile(r'(\d+)\s+ad\b', re.IGNORECASE)

        m = padrao_pt.search(html) or padrao_en.search(html)
        if m:
            n = int(m.group(1))
            resultado["ads_count"] = n
            resultado["anuncia"]   = n > 0
            resultado["status"]    = "verificado"
        else:
            resultado["status"] = "inconclusivo"

        return resultado

    except Exception as e:
        resultado["status"] = "erro_selenium"
        resultado["erro"]   = str(e)[:100]
        return resultado


if __name__ == "__main__":
    print("\n\U0001F50D Google Ads Transparency - versao final\n")

    try:
        import selenium
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip",
                               "install", "selenium", "webdriver-manager"])

    print("Iniciando Chrome (headless)...")
    try:
        driver = criar_driver()
        print("Chrome OK \u2705\n")
    except Exception:
        print(f"\n\u274C ERRO:\n{traceback.format_exc()}")
        input("\nPressione ENTER..."); sys.exit(1)

    resultados = []
    try:
        for t in TESTES:
            dominio  = t["dominio"]
            esperado = t["esperado"]
            print(f"Testando: {dominio}")
            r = verificar_google_ads(driver, dominio)
            acertou = r["anuncia"] == esperado
            icone   = "\u2705" if acertou else "\u274C"
            print(f"  {icone} anuncia={r['anuncia']} | "
                  f"ads={r['ads_count']} | status={r['status']}")
            if not acertou:
                print(f"  \u26A0\uFE0F  Esperado: {esperado}")
            resultados.append((dominio, r, esperado, acertou))
            time.sleep(2)
    except Exception:
        print(f"\n\u274C ERRO:\n{traceback.format_exc()}")
    finally:
        try: driver.quit()
        except Exception: pass
        print("\nChrome encerrado.")

    print(f"\n{'='*40}\nRESUMO\n{'='*40}")
    acertos = sum(1 for _,_,_,ok in resultados if ok)
    print(f"Acertos: {acertos}/{len(resultados)}")
    for dominio, r, esp, ok in resultados:
        print(f"  {'\u2705' if ok else '\u274C'} {dominio:40s} "
              f"anuncia={r['anuncia']} esperado={esp}")

    input("\nPressione ENTER para fechar...")
