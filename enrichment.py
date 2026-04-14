"""
enrichment.py — Orquestrador de enriquecimento.
Recebe dados básicos de uma imobiliária e roda todos os agentes em sequência.
Retorna o dict completo pronto para inserir no banco.
"""

import time
from agents.instagram_agent import validar_perfil as validar_instagram
from agents.site_agent import verificar_site
from agents.ads_agent import verificar_meta_ads, verificar_google_ads, determinar_abordagem
from agents.cnpj_agent import buscar_cnpj_por_nome, consultar_cnpj
from agents.whatsapp_agent import processar_telefones
from agents.portals_agent import verificar_portais


def enriquecer_imobiliaria(
    nome: str,
    cidade: str,
    instagram_url: str = None,
    site_url: str = None,
    cnpj: str = None,
    meta_token: str = None,
    log_callback=None,
) -> dict:
    """
    Enriquece uma imobiliária com todos os dados necessários.
    log_callback(msg): função opcional para reportar progresso em tempo real.
    Retorna dict com todos os campos para inserção no banco.
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    resultado = {
        "nome": nome,
        "cidade": cidade,
        "instagram_url": instagram_url,
        "site_url": site_url,
        "cnpj": cnpj,
        # Defaults
        "instagram_ativo": 0,
        "instagram_ultimo_post": None,
        "site_funcional": 0,
        "anuncia_meta": 0,
        "qtd_anuncios_meta": 0,
        "anuncia_google": 0,
        "qtd_anuncios_google": 0,
        "portais": [],
        "responsavel": None,
        "email": None,
        "telefones": [],
        "whatsapp_link": None,
        "whatsapp_validado": 0,
        "material_valor": "GOOGLE",
        "abordagem_tipo": "A",
        "descartado": 0,
        "motivo_descarte": None,
        "erros": [],
    }

    # ── 1. Instagram ──────────────────────────────────────────────────────────
    if instagram_url:
        log(f"🔍 Verificando Instagram: {instagram_url}")
        ig = validar_instagram(instagram_url)
        resultado["instagram_url"] = ig["url_normalizada"]
        resultado["instagram_ativo"] = 1 if ig["ativo"] else 0
        resultado["instagram_ultimo_post"] = ig["ultimo_post"]

        if ig["ativo"] is False and ig["existe"]:
            resultado["descartado"] = 1
            resultado["motivo_descarte"] = f"Instagram inativo: {ig['motivo']}"
            log(f"  ❌ Descartado: {ig['motivo']}")
            return resultado
        elif ig["ativo"] is None:
            log(f"  ⚠️ Instagram: {ig['motivo']} — continuando")
        else:
            log(f"  ✅ Instagram ativo (último post: {ig['ultimo_post'] or 'não detectado'})")

    time.sleep(0.5)

    # ── 2. Site ───────────────────────────────────────────────────────────────
    if site_url:
        log(f"🌐 Verificando site: {site_url}")
        site = verificar_site(site_url)
        resultado["site_funcional"] = 1 if site["funcional"] else 0
        resultado["site_url"] = site["url_final"] or site_url
        if not site["funcional"]:
            log(f"  ⚠️ Site fora do ar: {site['motivo']}")
        else:
            log(f"  ✅ Site funcional")

    time.sleep(0.5)

    # ── 3. Meta Ads ───────────────────────────────────────────────────────────
    log(f"📢 Verificando Meta Ads Library...")
    meta = verificar_meta_ads(nome, site_url, meta_token)
    if meta.get("descartado"):
        resultado["descartado"] = 1
        resultado["motivo_descarte"] = f"Muitos anúncios Meta ({meta['quantidade']} > 8)"
        resultado["anuncia_meta"] = 1
        resultado["qtd_anuncios_meta"] = meta["quantidade"]
        log(f"  ❌ Descartado: {meta['quantidade']} anúncios Meta (limite: 8)")
        return resultado

    resultado["anuncia_meta"] = 1 if meta["anuncia"] else 0
    resultado["qtd_anuncios_meta"] = meta.get("quantidade", 0)
    if meta.get("erro"):
        resultado["erros"].append(f"Meta Ads: {meta['erro']}")
        log(f"  ⚠️ Meta Ads: {meta['erro']}")
    else:
        status_meta = f"{meta['quantidade']} anúncios" if meta["anuncia"] else "sem anúncios"
        log(f"  ✅ Meta Ads: {status_meta}")

    time.sleep(1)

    # ── 4. Google Ads ─────────────────────────────────────────────────────────
    log(f"🔎 Verificando Google Ads...")
    google = verificar_google_ads(nome, cidade, site_url)
    if google.get("descartado"):
        resultado["descartado"] = 1
        resultado["motivo_descarte"] = f"Muitos anúncios Google ({google['quantidade']} > 15)"
        resultado["anuncia_google"] = 1
        resultado["qtd_anuncios_google"] = google["quantidade"]
        log(f"  ❌ Descartado: {google['quantidade']} anúncios Google (limite: 15)")
        return resultado

    resultado["anuncia_google"] = 1 if google["anuncia"] else 0
    resultado["qtd_anuncios_google"] = google.get("quantidade", 0)
    if google.get("erro"):
        resultado["erros"].append(f"Google Ads: {google['erro']}")
        log(f"  ⚠️ Google Ads: {google['erro']}")
    else:
        status_google = f"{google['quantidade']} anúncios" if google["anuncia"] else "sem anúncios"
        log(f"  ✅ Google Ads: {status_google}")

    # ── 5. Abordagem ──────────────────────────────────────────────────────────
    resultado["abordagem_tipo"] = determinar_abordagem(meta, google)
    log(f"📝 Abordagem definida: {resultado['abordagem_tipo']}")

    time.sleep(0.5)

    # ── 6. Portais ────────────────────────────────────────────────────────────
    log(f"🏠 Verificando portais imobiliários...")
    portais = verificar_portais(nome, cidade, site_url)
    resultado["portais"] = portais["portais_encontrados"]
    if portais["portais_encontrados"]:
        log(f"  ✅ Portais: {', '.join(portais['portais_encontrados'])}")
    else:
        log(f"  ℹ️ Não encontrado em portais")

    time.sleep(0.5)

    # ── 7. CNPJ ───────────────────────────────────────────────────────────────
    log(f"📋 Buscando CNPJ...")
    cnpj_dados = {}

    if not cnpj:
        cnpj = buscar_cnpj_por_nome(nome, cidade)
        if cnpj:
            log(f"  ✅ CNPJ encontrado: {cnpj}")
        else:
            log(f"  ⚠️ CNPJ não encontrado automaticamente")

    if cnpj:
        resultado["cnpj"] = cnpj
        log(f"  🔍 Consultando dados do CNPJ...")
        cnpj_dados = consultar_cnpj(cnpj)
        if "erro" not in cnpj_dados:
            resultado["responsavel"] = cnpj_dados.get("responsavel")
            resultado["email"] = cnpj_dados.get("email")
            resultado["telefones"] = cnpj_dados.get("telefones", [])
            if cnpj_dados.get("uf"):
                resultado["uf"] = cnpj_dados["uf"]
            log(f"  ✅ CNPJ: {cnpj_dados.get('razao_social', '')} | "
                f"Responsável: {cnpj_dados.get('responsavel', 'N/A')}")
        else:
            log(f"  ⚠️ CNPJ: {cnpj_dados['erro']}")
            resultado["erros"].append(f"CNPJ: {cnpj_dados['erro']}")

    time.sleep(0.5)

    # ── 8. WhatsApp ───────────────────────────────────────────────────────────
    telefones = resultado.get("telefones", [])

    # Tenta também extrair do site se tiver poucos telefones
    if len(telefones) < 2 and resultado.get("site_url"):
        tel_site = _extrair_telefones_site(resultado["site_url"])
        for t in tel_site:
            if t not in telefones:
                telefones.append(t)
        resultado["telefones"] = telefones

    if telefones:
        log(f"📱 Verificando WhatsApp ({len(telefones)} número(s))...")
        wpp = processar_telefones(telefones, cidade)
        resultado["whatsapp_link"] = wpp["whatsapp_link"]
        resultado["whatsapp_validado"] = 1 if wpp["whatsapp_validado"] else 0
        if wpp["whatsapp_link"]:
            status = "validado" if wpp["whatsapp_validado"] else "não confirmado"
            log(f"  ✅ WhatsApp {status}: {wpp['numero_whatsapp']}")
        else:
            log(f"  ⚠️ Nenhum WhatsApp encontrado")
    else:
        log(f"  ⚠️ Sem telefones para verificar WhatsApp")

    log(f"✅ Enriquecimento concluído: {nome}")
    return resultado


def _extrair_telefones_site(url: str) -> list[str]:
    """Tenta extrair telefones da página principal do site."""
    import requests
    import re
    try:
        r = requests.get(url, timeout=10,
                         headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            # Padrões de telefone brasileiro
            padrao = r"(?:\+55\s?)?(?:\(?\d{2}\)?\s?)(?:9\s?)?\d{4}[-\s]?\d{4}"
            matches = re.findall(padrao, r.text)
            limpos = []
            for m in matches[:5]:
                limpo = re.sub(r"\D", "", m)
                if 10 <= len(limpo) <= 13 and limpo not in limpos:
                    limpos.append(limpo)
            return limpos
    except Exception:
        pass
    return []
