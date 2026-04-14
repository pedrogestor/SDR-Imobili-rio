"""
teste_meta_selenium.py v5 — Extrai page_id do HTML do autocomplete
e navega direto para a URL do anunciante. Sem precisar clicar no dropdown.
"""

import re, time, json
from urllib.parse import quote

NOME      = "F. Veiga Imóveis"
IG_HANDLE = "fveigaimoveis"

def sep(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

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


def digitar_no_campo(driver, texto: str) -> bool:
    """Encontra o campo de busca e digita letra por letra."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # Aguarda qualquer input aparecer na página
    seletores = [
        "input[role='combobox']",
        "input[type='search']",
        "input[placeholder*='Pesquisar']",
        "input[placeholder*='Search']",
        "input[aria-label*='Pesquisar']",
        "input[aria-label*='Search']",
        "input[class*='search']",
        "input[data-testid*='search']",
        "input",  # fallback: qualquer input visível
    ]

    campo = None
    for sel in seletores:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    campo = el
                    print(f"  Campo encontrado: {sel}")
                    break
            if campo:
                break
        except Exception:
            continue

    if not campo:
        print("  ❌ Campo de busca não encontrado")
        return False

    try:
        campo.click()
        time.sleep(0.3)
        campo.clear()
        time.sleep(0.2)
        for char in texto:
            campo.send_keys(char)
            time.sleep(0.1)
        print(f"  Digitado: '{texto}'")
        return True
    except Exception as e:
        print(f"  Erro ao digitar: {e}")
        return False


def extrair_page_ids_do_autocomplete(html: str) -> list[dict]:
    """
    Extrai sugestões do autocomplete a partir do JSON embutido na página.
    O Meta embute os dados das sugestões em blocos de JSON no HTML.
    """
    sugestoes = []

    # Padrão 1: procura blocos com page_id próximos de page_name no HTML
    for m in re.finditer(r'"page_id"\s*:\s*"(\d+)"', html):
        pos    = m.start()
        trecho = html[max(0, pos-400):pos+600]

        # Extrai page_name
        name_m = re.search(r'"page_name"\s*:\s*"([^"]+)"', trecho)
        if not name_m:
            continue
        raw_name = name_m.group(1)
        try:
            page_name = raw_name.encode('utf-8').decode('unicode_escape')
        except Exception:
            page_name = raw_name

        pid = m.group(1)
        if not any(s["page_id"] == pid for s in sugestoes):
            sugestoes.append({"page_id": pid, "page_name": page_name})

    # Padrão 2: blocos JSON com "id" + "name" próximos
    # (formato alternativo que a Meta usa no autocomplete)
    for m in re.finditer(r'"id"\s*:\s*"(\d{10,20})"', html):
        pos    = m.start()
        trecho = html[max(0, pos-100):pos+300]
        name_m = re.search(r'"name"\s*:\s*"([^"]{3,60})"', trecho)
        if name_m:
            raw_name = name_m.group(1)
            try:
                page_name = raw_name.encode('utf-8').decode('unicode_escape')
            except Exception:
                page_name = raw_name
            pid = m.group(1)
            if not any(s["page_id"] == pid for s in sugestoes):
                sugestoes.append({"page_id": pid, "page_name": page_name})

    return sugestoes


def contar_anuncios_pagina(driver, page_id: str) -> dict:
    """
    Navega direto para a página do anunciante na Ads Library.
    Retorna contagem de anúncios.
    """
    url = (f"https://www.facebook.com/ads/library/"
           f"?active_status=active&ad_type=all&country=BR"
           f"&view_all_page_id={page_id}")
    print(f"  Abrindo página do anunciante: {url}")
    driver.get(url)
    time.sleep(4)

    html = driver.page_source

    # Conta quantos ad_archive_id aparecem
    ad_ids = re.findall(r'"ad_archive_id"\s*:\s*"(\d+)"', html)
    unicos = list(set(ad_ids))

    # Verifica "sem anúncios"
    sem_anuncios = any(s in html.lower() for s in [
        "no ads found", "nenhum anúncio", "0 results",
        "didn't find", "não encontramos",
    ])

    return {
        "ad_count":     len(unicos),
        "sem_anuncios": sem_anuncios,
        "html_len":     len(html),
    }


if __name__ == "__main__":
    print(f"\n🔍 Meta Ads Library v5 — Extrai page_id do autocomplete")
    print(f"   Empresa: {NOME} | Handle: @{IG_HANDLE}\n")

    try:
        from selenium import webdriver
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable,"-m","pip","install",
                               "selenium","webdriver-manager"])

    driver = criar_driver()
    print("Chrome aberto ✅")

    try:
        # ── Etapa 1: abre Ads Library e digita o handle ───────────────────────
        sep("Etapa 1 — Digita handle e captura autocomplete")
        url_base = ("https://www.facebook.com/ads/library/"
                    "?active_status=active&ad_type=all&country=BR")
        driver.get(url_base)
        time.sleep(3)

        ok = digitar_no_campo(driver, IG_HANDLE)
        if not ok:
            print("Falhou ao digitar. Encerrando.")
        else:
            # Aguarda autocomplete carregar
            time.sleep(3)
            driver.save_screenshot("debug_autocomplete.png")
            print("  Screenshot: debug_autocomplete.png")

            html_autocomplete = driver.page_source
            print(f"  HTML após digitar: {len(html_autocomplete)} chars")

            # Salva para análise
            with open("debug_autocomplete.html", "w", encoding="utf-8") as f:
                f.write(html_autocomplete)
            print("  HTML salvo: debug_autocomplete.html")

            # ── Etapa 2: extrai page_id do HTML do autocomplete ───────────────
            sep("Etapa 2 — Extrai page_id do HTML")
            sugestoes = extrair_page_ids_do_autocomplete(html_autocomplete)
            print(f"  Sugestões encontradas: {len(sugestoes)}")
            for s in sugestoes[:10]:
                print(f"    → [{s['page_id']}] {s['page_name']!r}")

            # Também tenta extrair procurando no HTML por qualquer page_id
            # próximo do handle
            handle_pos = html_autocomplete.lower().find(IG_HANDLE.lower())
            if handle_pos > 0:
                trecho_handle = html_autocomplete[
                    max(0, handle_pos-500):handle_pos+500]
                pid_m = re.search(r'"(?:page_id|id)"\s*:\s*"(\d{10,20})"',
                                  trecho_handle)
                if pid_m:
                    print(f"\n  page_id próximo ao handle no HTML: "
                          f"{pid_m.group(1)}")
                    sugestoes.append({
                        "page_id":   pid_m.group(1),
                        "page_name": "extraído por proximidade ao handle",
                    })

            if not sugestoes:
                print("  ⚠️ Nenhum page_id encontrado no HTML do autocomplete")
                print("  Verificando trechos ao redor do handle...")
                if handle_pos > 0:
                    print(f"  Handle encontrado na posição {handle_pos}")
                    print(f"  Trecho: {html_autocomplete[handle_pos-100:handle_pos+200]}")
                else:
                    print("  Handle NÃO encontrado no HTML")
            else:
                # ── Etapa 3: navega para a página do anunciante ───────────────
                sep("Etapa 3 — Conta anúncios pelo page_id")
                for s in sugestoes[:3]:
                    print(f"\n  Testando: {s['page_name']!r} (id={s['page_id']})")
                    ads = contar_anuncios_pagina(driver, s["page_id"])
                    print(f"  Anúncios ativos: {ads['ad_count']}")
                    print(f"  Sem anúncios:    {ads['sem_anuncios']}")
                    print(f"  HTML recebido:   {ads['html_len']} chars")

        sep("RESUMO FINAL")
        print("Verifique os arquivos de debug:")
        print("  debug_autocomplete.png — screenshot do Chrome")
        print("  debug_autocomplete.html — HTML completo após digitar")

    finally:
        driver.quit()
        print("\nChrome encerrado.")

    input("\nPressione ENTER para fechar...")
