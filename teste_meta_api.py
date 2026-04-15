"""
teste_meta_api.py v2 — Testa o token Meta e as permissões disponíveis.
Descobre o que é possível fazer com a Ads Library API.
"""

import requests, json
from urllib.parse import quote

TOKEN = "EAANhXQbKm1IBRBU1HujpNMoZCZAWUtnDHNv5dAN5W9d8DQXxT9YfqJijHlg6xIZBjCYQILf7oH5VZBZCZAQBLUlNy1W0CvzUOhqbndYMyJUSAC4c0wZB8zuOjEKPAFLC3i7RU9VEtdnaHabCsevODgXk7TZCN1wmW8f4ilhj9ZCk9MRe0xHQhsPcOI8Lc5BSIIWBTS3iovGqfhMUHqgC7MTtsv5vHGZBgvp1AEAVZAL"

def sep(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

def req(url, label):
    r = requests.get(url, timeout=15)
    print(f"  Status: {r.status_code}")
    try:
        data = r.json()
        if "error" in data:
            print(f"  ❌ Erro: {data['error'].get('message')}")
            print(f"     Código: {data['error'].get('code')} | Tipo: {data['error'].get('type')}")
            return None
        return data
    except Exception as e:
        print(f"  ❌ Parse error: {e}")
        return None

# ── 1. Token válido? ──────────────────────────────────────────────────────────
sep("1 — Validade do token")
data = req(f"https://graph.facebook.com/v19.0/me?access_token={TOKEN}", "me")
if data:
    print(f"  ✅ Token válido")
    print(f"     ID: {data.get('id')} | Nome: {data.get('name')}")

# ── 2. Permissões do token ────────────────────────────────────────────────────
sep("2 — Permissões disponíveis")
data = req(f"https://graph.facebook.com/v19.0/me/permissions?access_token={TOKEN}", "permissions")
if data:
    perms = data.get("data", [])
    granted = [p["permission"] for p in perms if p.get("status") == "granted"]
    denied  = [p["permission"] for p in perms if p.get("status") != "granted"]
    print(f"  Permissões concedidas ({len(granted)}): {granted}")
    if denied:
        print(f"  Negadas: {denied}")
    tem_ads_read = "ads_read" in granted
    print(f"\n  ads_read: {'✅ SIM' if tem_ads_read else '❌ NÃO'}")

# ── 3. Busca de página por nome ───────────────────────────────────────────────
sep("3 — Busca de página: 'F. Veiga Imóveis Goiania'")
url3 = (f"https://graph.facebook.com/v19.0/search"
        f"?q={quote('F. Veiga Imóveis Goiania')}&type=page"
        f"&fields=id,name,fan_count,link"
        f"&access_token={TOKEN}")
data = req(url3, "search_page")
page_id = None
if data:
    pages = data.get("data", [])
    print(f"  Páginas encontradas: {len(pages)}")
    for p in pages[:5]:
        print(f"    → [{p.get('id')}] {p.get('name')} | fans={p.get('fan_count','?')}")
    if pages:
        page_id = pages[0]["id"]

# ── 4. Ads Archive por page_id ────────────────────────────────────────────────
sep("4 — Ads Archive por page_id")
if page_id:
    url4 = (f"https://graph.facebook.com/v19.0/ads_archive"
            f"?access_token={TOKEN}"
            f"&ad_reached_countries=[\"BR\"]"
            f"&search_page_ids={page_id}"
            f"&ad_active_status=ACTIVE"
            f"&fields=id,page_name,ad_delivery_start_time"
            f"&limit=5")
    data = req(url4, "ads_archive_page")
    if data:
        ads = data.get("data", [])
        print(f"  Anúncios ativos: {len(ads)}")
        for ad in ads[:3]:
            print(f"    → {ad.get('page_name')} | início={ad.get('ad_delivery_start_time')}")
else:
    print("  Pulado — sem page_id do teste 3")

# ── 5. Ads Archive por keyword ────────────────────────────────────────────────
sep("5 — Ads Archive por keyword: 'F. Veiga Imóveis'")
url5 = (f"https://graph.facebook.com/v19.0/ads_archive"
        f"?access_token={TOKEN}"
        f"&ad_reached_countries=[\"BR\"]"
        f"&search_terms={quote('F. Veiga Imóveis')}"
        f"&ad_active_status=ACTIVE"
        f"&fields=id,page_name,ad_delivery_start_time,ad_snapshot_url"
        f"&limit=5")
data = req(url5, "ads_archive_keyword")
if data:
    ads = data.get("data", [])
    print(f"  Anúncios encontrados: {len(ads)}")
    for ad in ads[:3]:
        print(f"    → {ad.get('page_name')} | início={ad.get('ad_delivery_start_time')}")

# ── 6. Ads Archive por domínio ────────────────────────────────────────────────
sep("6 — Ads Archive por domínio: fveigaimoveis.com.br")
url6 = (f"https://graph.facebook.com/v19.0/ads_archive"
        f"?access_token={TOKEN}"
        f"&ad_reached_countries=[\"BR\"]"
        f"&search_terms=fveigaimoveis.com.br"
        f"&ad_active_status=ACTIVE"
        f"&fields=id,page_name,ad_snapshot_url"
        f"&limit=5")
data = req(url6, "ads_archive_domain")
if data:
    ads = data.get("data", [])
    print(f"  Anúncios encontrados: {len(ads)}")
    for ad in ads[:3]:
        print(f"    → {ad.get('page_name')}")

# ── 7. Teste com imobiliária que sabemos que anuncia ─────────────────────────
sep("7 — Viver Imóveis BH (sabemos que anuncia)")
url7 = (f"https://graph.facebook.com/v19.0/ads_archive"
        f"?access_token={TOKEN}"
        f"&ad_reached_countries=[\"BR\"]"
        f"&search_terms={quote('Viver Imóveis BH')}"
        f"&ad_active_status=ACTIVE"
        f"&fields=id,page_name,ad_delivery_start_time"
        f"&limit=5")
data = req(url7, "viver_imoveis")
if data:
    ads = data.get("data", [])
    print(f"  Anúncios encontrados: {len(ads)}")
    for ad in ads[:3]:
        print(f"    → {ad.get('page_name')} | início={ad.get('ad_delivery_start_time')}")
    if ads:
        print(f"  ✅ API retornou resultados — ads_read funcionando")

print(f"\n{'='*60}")
print("Concluído.")
input("\nENTER para fechar...")
