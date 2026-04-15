"""
pipeline.py — Pipeline contínuo orientado por meta de leads válidos.
Continua buscando candidatos até atingir a quantidade solicitada ou esgotar.

Critério de esgotamento: 3 UFs consecutivas sem nenhum candidato novo elegível.
"""

import re, time
from typing import Callable
import database as db

# Número de UFs consecutivas sem candidato novo antes de declarar esgotamento
UFS_VAZIAS_PARA_ESGOTAR = 3


def gerar_lista(
    lista_id: int,
    quantidade_desejada: int,
    log_cb: Callable[[str], None] = None,
) -> dict:
    """
    Pipeline completo orientado por meta.
    Para quando: aprovados >= quantidade_desejada  OU  esgotamento detectado.
    """
    def log(msg):
        if log_cb:
            log_cb(msg)

    from agents.discovery_agent import gerar_candidatos, UFS
    from agents.enrichment_agent import criar_driver, enriquecer

    import time as _t
    _inicio_geracao = _t.time()

    db.atualizar_lista(lista_id, {"status": "gerando"})

    aprovados    = 0
    descartados  = 0
    processados  = 0

    # Conjunto de CNPJs já vistos nesta execução (evita reprocessar)
    cnpjs_vistos: set[str] = set()

    # Rastreamento de esgotamento por UF
    uf_atual_idx        = 0
    ufs_sem_novos       = 0  # contador de UFs consecutivas sem candidato novo

    log(f"🚀 Meta: {quantidade_desejada} leads aprovados")
    log(f"   Critério de esgotamento: {UFS_VAZIAS_PARA_ESGOTAR} UFs consecutivas sem candidatos\n")

    # Inicia Chrome
    log("🌐 Iniciando Chrome...")
    try:
        driver = criar_driver()
        log("   Chrome OK\n")
    except Exception as e:
        log(f"❌ Não foi possível iniciar o Chrome: {e}")
        db.atualizar_lista(lista_id, {"status": "erro"})
        return {"aprovados": 0, "descartados": 0, "erro": str(e)}

    try:
        candidatos_gen = gerar_candidatos(
            vistos_cnpjs=cnpjs_vistos,
            log_cb=log,
        )

        uf_candidatos_este_ciclo = 0
        uf_nome_anterior         = None

        for cand in candidatos_gen:
            if aprovados >= quantidade_desejada:
                log(f"\n🎯 Meta atingida: {aprovados}/{quantidade_desejada} aprovados!")
                break

            nome   = cand.get("nome", "").strip()
            cidade = cand.get("cidade", "").strip()
            cnpj   = re.sub(r"\D", "", cand.get("cnpj", ""))
            uf     = cand.get("estado", "")

            if not nome:
                continue

            # Detecta mudança de UF para rastrear esgotamento
            if uf != uf_nome_anterior:
                if uf_nome_anterior is not None:
                    if uf_candidatos_este_ciclo == 0:
                        ufs_sem_novos += 1
                        log(f"   ⚠️ UF {uf_nome_anterior} sem candidatos elegíveis "
                            f"({ufs_sem_novos}/{UFS_VAZIAS_PARA_ESGOTAR})")
                    else:
                        ufs_sem_novos = 0  # reseta contador
                uf_candidatos_este_ciclo = 0
                uf_nome_anterior = uf

            # Verifica esgotamento
            if ufs_sem_novos >= UFS_VAZIAS_PARA_ESGOTAR:
                log(f"\n⚠️ Esgotamento declarado após {UFS_VAZIAS_PARA_ESGOTAR} "
                    f"UFs consecutivas sem candidatos elegíveis.")
                break

            # Verifica elegibilidade (reprovadas + abordadas + lista atual)
            inelegivel, motivo_ineg = db.checar_inelegivel(
                nome, cidade, cnpj=cnpj, lista_id=lista_id)
            if inelegivel:
                log(f"  ⟳ Ignorado ({motivo_ineg}): {nome}")
                continue

            processados += 1
            uf_candidatos_este_ciclo += 1
            log(f"\n─── [{processados}] {nome} ({cidade}/{uf})")

            # Enriquece (site + Instagram)
            resultado = enriquecer(driver, nome, cidade, log=log)

            # ── Early exit: sem site → descarta imediatamente ─────────────
            # Exceto se o motivo foi bloqueio do Google (não descartar nesse caso)
            google_bloqueou = (resultado.review_flags and
                               "site:google_bloqueado" in resultado.review_flags)

            if resultado.sem_site and not google_bloqueou:
                lead_id = db.inserir_lead(lista_id, _montar_lead(
                    cand, resultado, ads=None))
                db.atualizar_lead(lead_id, {
                    "approved": 0, "discard_reason": "sem_site"})
                db.inserir_reprovada(nome=nome, cidade=cidade,
                                     motivo="sem_site", cnpj=cnpj,
                                     lista_id=lista_id)
                descartados += 1
                log(f"  ❌ DESCARTADO — sem site")
                db.atualizar_lista(lista_id, {
                    "approved_quantity": aprovados,
                    "discarded_quantity": descartados})
                continue

            if google_bloqueou:
                # Salva sem aprovar — fica como pendente para reprocessamento
                lead_id = db.inserir_lead(lista_id, _montar_lead(
                    cand, resultado, ads=None))
                db.atualizar_lead(lead_id, {
                    "approved": 0,
                    "discard_reason": "google_bloqueado_revisar"})
                log(f"  ⚠️ PENDENTE — Google bloqueou busca, revisar manualmente")
                db.atualizar_lista(lista_id, {
                    "approved_quantity": aprovados,
                    "discarded_quantity": descartados})
                continue

            # ── Early exit: sem Instagram → descarta imediatamente ────────
            if resultado.sem_instagram:
                motivo = resultado.motivo_rejeicao_ig or "sem_instagram"
                lead_id = db.inserir_lead(lista_id, _montar_lead(
                    cand, resultado, ads=None))
                db.atualizar_lead(lead_id, {
                    "approved": 0, "discard_reason": motivo})
                db.inserir_reprovada(nome=nome, cidade=cidade,
                                     motivo=motivo, cnpj=cnpj,
                                     instagram_handle=resultado.ig_handle,
                                     site_url=resultado.site_url,
                                     lista_id=lista_id)
                descartados += 1
                log(f"  ❌ DESCARTADO — {motivo}")
                db.atualizar_lista(lista_id, {
                    "approved_quantity": aprovados,
                    "discarded_quantity": descartados})
                continue

            # ── Verifica anúncios pagos (só chega aqui se tem site + IG) ──
            from agents.ads_checker import verificar_anuncios
            ads = verificar_anuncios(
                driver, nome, cidade,
                site_url=resultado.site_url,
                log=log,
            )

            # ── Critério eliminatório: muitos anúncios Google = fora do perfil ──
            MAX_GOOGLE_ADS_ACEITOS = 10
            if (ads.google_ads_active and
                    ads.google_ads_count_estimate > MAX_GOOGLE_ADS_ACEITOS):
                lead_id = db.inserir_lead(lista_id, _montar_lead(cand, resultado, ads))
                motivo_g = (f"muitos_anuncios_google:"
                            f"{ads.google_ads_count_estimate}")
                db.atualizar_lead(lead_id, {
                    "approved": 0, "discard_reason": motivo_g})
                db.inserir_reprovada(nome=nome, cidade=cidade,
                                     motivo=motivo_g, cnpj=cnpj,
                                     instagram_handle=resultado.ig_handle,
                                     site_url=resultado.site_url,
                                     lista_id=lista_id)
                descartados += 1
                log(f"  ❌ DESCARTADO — {motivo_g} (limite: {MAX_GOOGLE_ADS_ACEITOS})")
                db.atualizar_lista(lista_id, {
                    "approved_quantity": aprovados,
                    "discarded_quantity": descartados})
                continue

            # Salva dados completos do candidato no banco
            lead_id = db.inserir_lead(lista_id, _montar_lead(cand, resultado, ads))

            # Decide aprovação
            motivo_reprovacao = _avaliar_aprovacao(resultado)

            if motivo_reprovacao:
                # Reprovado — vai para tabela global de reprovadas
                db.atualizar_lead(lead_id, {
                    "approved":       0,
                    "discard_reason": motivo_reprovacao,
                })
                db.inserir_reprovada(
                    nome=nome, cidade=cidade, motivo=motivo_reprovacao,
                    cnpj=cnpj,
                    instagram_handle=resultado.ig_handle,
                    site_url=resultado.site_url,
                    lista_id=lista_id,
                )
                descartados += 1
                log(f"  ❌ REPROVADO — {motivo_reprovacao}")
            else:
                # Aprovado
                db.atualizar_lead(lead_id, {
                    "approved":       1,
                    "discard_reason": None,
                })
                # Marca automaticamente como abordada
                db.inserir_abordada(
                    nome=nome, cidade=cidade, lista_id=lista_id,
                    cnpj=cnpj,
                    instagram_handle=resultado.ig_handle,
                    site_url=resultado.site_url,
                )
                aprovados += 1
                log(f"  ✅ APROVADO ({aprovados}/{quantidade_desejada}) — registrado em abordadas")

            # Atualiza contadores na lista
            db.atualizar_lista(lista_id, {
                "approved_quantity":  aprovados,
                "discarded_quantity": descartados,
            })

            time.sleep(0.5)

    finally:
        driver.quit()
        log("\n🔒 Chrome encerrado.")

    # Status final
    if aprovados >= quantidade_desejada:
        status_final = "concluida"
        log(f"\n🏁 Lista concluída: {aprovados} aprovados, {descartados} descartados.")
    else:
        status_final = "esgotada"
        log(f"\n⚠️ Lista esgotada: apenas {aprovados}/{quantidade_desejada} aprovados. "
            f"{descartados} descartados.")

    _segundos = int(_t.time() - _inicio_geracao)
    db.atualizar_lista(lista_id, {
        "status":             status_final,
        "approved_quantity":  aprovados,
        "discarded_quantity": descartados,
        "generation_seconds": _segundos,
    })

    return {
        "aprovados":   aprovados,
        "descartados": descartados,
        "processados": processados,
        "status":      status_final,
    }


def _avaliar_aprovacao(r) -> str | None:
    """Retorna None se aprovado, ou motivo de reprovação."""
    if r.erro:
        return f"erro_processamento: {r.erro[:60]}"
    if r.sem_site:
        return "sem_site"
    if r.sem_instagram:
        return r.motivo_rejeicao_ig or "sem_instagram"
    return None


def _montar_lead(cand: dict, resultado, ads=None) -> dict:
    """Monta dict completo para inserção no banco."""
    import re
    nome = cand.get("nome", "")
    cnpj = re.sub(r"\D", "", cand.get("cnpj", ""))
    return {
        "nome_imobiliaria":      nome,
        "cidade":                cand.get("cidade"),
        "estado":                cand.get("estado"),
        "cnpj":                  cnpj,
        "razao_social":          cand.get("razao_social"),
        "email":                 cand.get("email"),
        "telefone_raw":          cand.get("telefone_1"),
        "responsaveis_json":     cand.get("socios", []),
        "responsavel_principal": cand.get("socios", [None])[0]
                                 if cand.get("socios") else None,
        "site_url":              resultado.site_url if resultado else None,
        "site_ok":               1 if (resultado and resultado.site_url) else 0,
        "instagram_url":         resultado.instagram_url if resultado else None,
        "instagram_handle":      resultado.ig_handle if resultado else None,
        "instagram_seguidores":  resultado.ig_seguidores if resultado else None,
        "instagram_num_posts":   resultado.ig_num_posts if resultado else None,
        "last_post_date":        resultado.ig_ultimo_post if resultado else None,
        "weeks_since_last_post": resultado.ig_semanas if resultado else None,
        "advertise_meta":        1 if (ads and ads.meta_ads_active) else 0,
        "meta_ads_count":        (ads.meta_ads_count_estimate if ads else 0),
        "advertise_google":      1 if (ads and ads.google_ads_active) else 0,
        "google_ads_count":      (ads.google_ads_count_estimate if ads else 0),
        "dominant_channel":      (ads.dominant_channel if ads else None),
        "portals_json":          [],
        "raw_debug_json":        {
            "fonte":             "minhareceita",
            "ig_metodo":         resultado.ig_metodo if resultado else None,
            "ig_motivo":         resultado.motivo_rejeicao_ig if resultado else None,
            "ig_origem":         resultado.ig_origem if resultado else None,
            "ig_reciproco":      resultado.ig_reciproco if resultado else False,
            "site_score":        resultado.site_score if resultado else 0,
            "site_sinais":       resultado.site_sinais if resultado else [],
            "review_flags":      resultado.review_flags if resultado else [],
            "ads_summary":       ads.ads_summary if ads else None,
            "ads_meta_status":   ads.meta_ads_status if ads else None,
            "ads_google_status": ads.google_ads_status if ads else None,
            "ads_confidence":    ads.confidence_score if ads else None,
            "erro":              resultado.erro if resultado else None,
        },
        "approved":              0,
        "discard_reason":        None,
    }
