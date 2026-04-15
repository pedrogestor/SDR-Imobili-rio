import requests, sys, traceback

TOKEN = "EAANhXQbKm1IBRBU1HujpNMoZCZAWUtnDHNv5dAN5W9d8DQXxT9YfqJijHlg6xIZBjCYQILf7oH5VZBZCZAQBLUlNy1W0CvzUOhqbndYMyJUSAC4c0wZB8zuOjEKPAFLC3i7RU9VEtdnaHabCsevODgXk7TZCN1wmW8f4ilhj9ZCk9MRe0xHQhsPcOI8Lc5BSIIWBTS3iovGqfhMUHqgC7MTtsv5vHGZBgvp1AEAVZAL"

try:
    # Lê o debug_token corretamente
    print("\n[DEBUG TOKEN - ver app_id e detalhes]")
    r = requests.get(
        f"https://graph.facebook.com/debug_token"
        f"?input_token={TOKEN}&access_token={TOKEN}", timeout=15)
    d = r.json()
    if "data" in d:
        info = d["data"]
        print(f"  App ID:      {info.get('app_id')}")
        print(f"  App nome:    {info.get('application')}")
        print(f"  Tipo:        {info.get('type')}")
        print(f"  Expira em:   {info.get('expires_at')}")
        print(f"  Valido:      {info.get('is_valid')}")
        scopes = info.get("scopes", [])
        print(f"  Scopes ({len(scopes)}): {scopes}")
        print()
        app_id = info.get("app_id")

        # Verifica se o app tem acesso à Ads Library
        print("[VERIFICA ACESSO ADS LIBRARY]")
        r2 = requests.get(
            f"https://graph.facebook.com/v19.0/{app_id}"
            f"?fields=name,supported_platforms,app_domains"
            f"&access_token={TOKEN}", timeout=15)
        d2 = r2.json()
        if "error" in d2:
            print(f"  Nao conseguiu ler o app: {d2['error'].get('message')}")
        else:
            print(f"  Nome do app: {d2.get('name')}")
            print(f"  Plataformas: {d2.get('supported_platforms')}")
    else:
        print(f"  Resposta: {d}")

    # Teste direto: busca por page_name exato da Viver Imoveis
    print("\n[BUSCA PAGINA VIVER IMOVEIS BH]")
    r3 = requests.get(
        f"https://graph.facebook.com/v19.0/search"
        f"?q=Viver+Imoveis+BH&type=page"
        f"&fields=id,name,fan_count"
        f"&access_token={TOKEN}", timeout=15)
    d3 = r3.json()
    pages = d3.get("data", [])
    print(f"  Paginas encontradas: {len(pages)}")
    for p in pages[:5]:
        print(f"    [{p.get('id')}] {p.get('name')} | fans={p.get('fan_count','?')}")

    if pages:
        pid = pages[0]["id"]
        print(f"\n[ADS ARCHIVE PELO page_id={pid}]")
        r4 = requests.get(
            f"https://graph.facebook.com/v19.0/ads_archive"
            f"?access_token={TOKEN}"
            f"&ad_reached_countries=BR"
            f"&search_page_ids={pid}"
            f"&ad_active_status=ALL"
            f"&fields=id,page_name,ad_delivery_start_time"
            f"&limit=5", timeout=15)
        d4 = r4.json()
        if "error" in d4:
            print(f"  ERRO: {d4['error'].get('message')}")
            print(f"  Codigo: {d4['error'].get('code')}")
            # Codigo 200 = permissao negada para este endpoint
            # Codigo 100 = parametro invalido
            # Codigo 10 = nao tem acesso especial Ads Library
        else:
            ads = d4.get("data", [])
            print(f"  Anuncios: {len(ads)}")
            for ad in ads[:3]:
                print(f"    -> {ad}")

except Exception:
    print(traceback.format_exc())

print("\nConcluido.")
input("\nENTER para fechar...")
