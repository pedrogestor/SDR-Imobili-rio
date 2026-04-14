"""
teste_meta_api.py — Testa a integração com a Meta Ads Library API.
Roda no CMD: python teste_meta_api.py
"""

import requests
import json
from urllib.parse import quote

TOKEN = "EAANhXQbKm1IBRAOR2GZAVbFWZCAoDNi2xqqFmUrRZC5jIkQVzeOCd0kROYKdeIaBKZAy8ZAuvimcM1ZBpfITTFUHsyZAd5mNRowv5MxVMh8mtMdb97jQSSSZBZAarIp8N36xK73qXz9NCXZCElwkwsmDiAZBvDGrLRQ5DErUhAmVjZBhHZAZAQ4LQrhQXeG13ZCfeKmkpXDUMjnkvL0ZAMEPPWT66UrvJtCZBYHVToFMiIm8pqxuoeAUndVyvoKXjBrhUlHVYdw2R0sa0sUm3KA9Mxz7TyPF0YHNd"

# Imobiliárias reais para testar (empresas conhecidas com presença digital)
EMPRESAS_TESTE = [
    {"nome": "Lopes Consultoria de Imóveis",    "cidade": "São Paulo"},
    {"nome": "Brasil Brokers",                   "cidade": "Rio de Janeiro"},
    {"nome": "Imobiliária Castelo",              "cidade": "Belo Horizonte"},
]


def sep(titulo):
    print(f"\n{'='*60}")
    print(f"  {titulo}")
    print('='*60)


# ── Teste 1: Validade do token ─────────────────────────────────────────────

def testar_token():
    sep("TESTE 1 — Validade do token")
    url = f"https://graph.facebook.com/v19.0/me?access_token={TOKEN}"
    try:
        r = requests.get(url, timeout=10)
        print(f"Status: {r.status_code}")
        data = r.json()
        if "error" in data:
            err = data["error"]
            print(f"❌ ERRO: {err.get('message')}")
            print(f"   Tipo: {err.get('type')} | Código: {err.get('code')}")
            return False
        print(f"✅ Token válido — usuário: {data.get('name','?')} (id={data.get('id','?')})")
        return True
    except Exception as e:
        print(f"❌ Exceção: {e}")
        return False


# ── Teste 2: Busca de página pelo nome ────────────────────────────────────

def testar_busca_pagina(nome: str, cidade: str):
    sep(f"TESTE 2 — Busca de página: {nome}")
    query = f"{nome} {cidade}"
    url = (f"https://graph.facebook.com/v19.0/search"
           f"?q={quote(query)}&type=page"
           f"&fields=id,name,link,fan_count,location"
           f"&access_token={TOKEN}")
    print(f"URL: {url[:100]}...")
    try:
        r = requests.get(url, timeout=15)
        print(f"Status: {r.status_code}")
        data = r.json()
        if "error" in data:
            print(f"❌ ERRO API: {data['error'].get('message')}")
            return None
        pages = data.get("data", [])
        print(f"Páginas encontradas: {len(pages)}")
        for p in pages[:5]:
            print(f"  → [{p.get('id')}] {p.get('name')} | "
                  f"fans={p.get('fan_count','?')} | "
                  f"link={p.get('link','?')}")
        if pages:
            return pages[0]["id"]
        return None
    except Exception as e:
        print(f"❌ Exceção: {e}")
        return None


# ── Teste 3: Busca de anúncios ativos por page_id ─────────────────────────

def testar_ads_por_page(page_id: str, nome: str):
    sep(f"TESTE 3 — Anúncios ativos: {nome} (page_id={page_id})")
    url = (f"https://graph.facebook.com/v19.0/ads_archive"
           f"?access_token={TOKEN}"
           f"&ad_reached_countries=['BR']"
           f"&search_page_ids={page_id}"
           f"&ad_active_status=ACTIVE"
           f"&fields=id,ad_creative_body,ad_delivery_start_time,ad_snapshot_url"
           f"&limit=5")
    print(f"URL: {url[:100]}...")
    try:
        r = requests.get(url, timeout=15)
        print(f"Status: {r.status_code}")
        data = r.json()
        if "error" in data:
            err = data["error"]
            print(f"❌ ERRO: {err.get('message')}")
            print(f"   Tipo: {err.get('type')} | Código: {err.get('code')}")
            print(f"   Resposta completa: {json.dumps(data, indent=2)}")
            return
        ads = data.get("data", [])
        print(f"Anúncios ativos: {len(ads)}")
        for ad in ads:
            print(f"  → id={ad.get('id')} | "
                  f"início={ad.get('ad_delivery_start_time','?')}")
        if not ads:
            print("  (nenhum anúncio ativo encontrado para esta página)")
    except Exception as e:
        print(f"❌ Exceção: {e}")


# ── Teste 4: Ads Library sem page_id (busca por palavra-chave) ────────────

def testar_ads_por_keyword(termo: str):
    sep(f"TESTE 4 — Ads Library por keyword: {termo}")
    url = (f"https://graph.facebook.com/v19.0/ads_archive"
           f"?access_token={TOKEN}"
           f"&ad_reached_countries=['BR']"
           f"&search_terms={quote(termo)}"
           f"&ad_active_status=ACTIVE"
           f"&fields=id,page_name,ad_delivery_start_time"
           f"&limit=5")
    print(f"URL: {url[:100]}...")
    try:
        r = requests.get(url, timeout=15)
        print(f"Status: {r.status_code}")
        data = r.json()
        if "error" in data:
            print(f"❌ ERRO: {data['error'].get('message')}")
            print(f"   Resposta: {json.dumps(data['error'], indent=2)}")
            return
        ads = data.get("data", [])
        print(f"Anúncios encontrados: {len(ads)}")
        for ad in ads[:3]:
            print(f"  → página={ad.get('page_name','?')} | "
                  f"início={ad.get('ad_delivery_start_time','?')}")
    except Exception as e:
        print(f"❌ Exceção: {e}")


# ── Execução ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n🔍 Teste Meta Ads Library API\n")

    # Testa validade do token primeiro
    token_ok = testar_token()
    if not token_ok:
        print("\n⛔ Token inválido. Corrija o token antes de continuar.")
        input("\nPressione ENTER para fechar...")
        exit(1)

    # Testa busca + ads para cada empresa
    for emp in EMPRESAS_TESTE:
        page_id = testar_busca_pagina(emp["nome"], emp["cidade"])
        if page_id:
            testar_ads_por_page(page_id, emp["nome"])

    # Testa busca por keyword (não precisa de page_id)
    testar_ads_por_keyword("imobiliária São Paulo")

    sep("RESUMO")
    print("Se os testes 1-4 retornaram dados sem erro:")
    print("  ✅ A integração Meta Ads está funcionando")
    print()
    print("Se o teste 3 retornou erro 'requires Permissions':")
    print("  → O token precisa da permissão 'ads_read'")
    print("  → Gere novo token em developers.facebook.com")
    print("    → Graph API Explorer → User Token → ads_read")
    print()
    input("Pressione ENTER para fechar...")
