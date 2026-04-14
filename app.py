"""
app.py — SDR Imobiliário.
Pipeline orientado por meta: continua até N leads aprovados ou esgotamento.
"""

import streamlit as st
import pandas as pd
import io
from datetime import date
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent))

import database as db
from config import APP_TITLE, APP_ICON

st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON, layout="wide")
st.markdown("""
<style>
div[data-testid="metric-container"] {
    background:#0d1117; border:1px solid #21262d; border-radius:8px; padding:16px;
}
.badge { display:inline-block; padding:2px 10px; border-radius:4px;
         font-size:12px; font-weight:600; font-family:monospace; }
footer { visibility:hidden; }
</style>
""", unsafe_allow_html=True)

db.init_db()
if "pagina" not in st.session_state:
    st.session_state.pagina = "criar_lista"
if "lista_detalhe_id" not in st.session_state:
    st.session_state.lista_detalhe_id = None

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"# {APP_ICON} {APP_TITLE}")
    st.markdown("---")
    nav = {
        "criar_lista": "➕ Criar Lista",
        "listas":      "📋 Listas",
        "bases":       "🗃️ Bases Globais",
        "exportar":    "📥 Exportar",
        "config":      "⚙️ Configurações",
    }
    for key, label in nav.items():
        tipo = "primary" if st.session_state.pagina == key else "secondary"
        if st.button(label, use_container_width=True, type=tipo,
                     key=f"nav_{key}"):
            st.session_state.pagina = key
            st.session_state.lista_detalhe_id = None
            st.rerun()
    st.markdown("---")
    stats = db.get_stats()
    st.markdown(f"**Listas:** {stats['listas']}")
    st.markdown(f"**Leads aprovados:** {stats['leads_aprovados']}")
    st.markdown(f"**Reprovadas:** {stats['reprovadas']}")
    st.markdown(f"**Abordadas:** {stats['abordadas']}")


# ══════════════════════════════════════════════════════════════════════════════
# CRIAR LISTA
# ══════════════════════════════════════════════════════════════════════════════

def pagina_criar_lista():
    st.title("➕ Criar Lista de Prospecção")
    st.markdown(
        "Informe a **quantidade de leads aprovados** que deseja. "
        "O sistema continua buscando até atingir essa meta ou declarar esgotamento."
    )

    with st.form("form_criar"):
        col1, col2 = st.columns([2, 1])
        with col1:
            nome_lista = st.text_input("Nome da lista *",
                                       placeholder="Ex: Prospecção Semana 15")
        with col2:
            quantidade = st.number_input(
                "Leads aprovados desejados *",
                min_value=1, max_value=200, value=20, step=5,
                help="Apenas leads que passarem em todos os critérios contam."
            )

        st.info(
            "**Critérios mínimos de aprovação:**\n"
            "- Ter site oficial confirmado\n"
            "- Ter Instagram com ≥ 500 seguidores, ≥ 20 posts "
            "e último post (não fixado) dentro de 8 semanas\n\n"
            f"**Tempo estimado:** {int(quantidade) * 1} a "
            f"{int(quantidade) * 2} minutos em segundo plano."
        )
        submitted = st.form_submit_button("🚀 Gerar Lista", type="primary",
                                          use_container_width=True)

    if not submitted:
        return
    if not nome_lista.strip():
        st.error("Informe o nome da lista.")
        return

    lista_id = db.criar_lista(nome=nome_lista.strip(),
                              qtd=int(quantidade), cidades=[])

    st.subheader("⚙️ Pipeline em execução")

    # Contadores em tempo real
    cnt_col1, cnt_col2, cnt_col3, cnt_col4 = st.columns(4)
    cnt_aprovados   = cnt_col1.empty()
    cnt_descartados = cnt_col2.empty()
    cnt_meta        = cnt_col3.empty()
    cnt_status      = cnt_col4.empty()

    cnt_aprovados.metric("✅ Aprovados", 0)
    cnt_descartados.metric("❌ Descartados", 0)
    cnt_meta.metric("🎯 Meta", int(quantidade))
    cnt_status.metric("📊 Status", "Rodando")

    log_ph = st.empty()
    logs   = []

    def log_cb(msg: str):
        logs.append(msg)
        log_ph.markdown("```\n" + "\n".join(logs[-50:]) + "\n```")
        # Atualiza contadores a partir do banco
        try:
            cont = db.contar_leads(lista_id)
            cnt_aprovados.metric("✅ Aprovados", cont["aprovados"])
            cnt_descartados.metric("❌ Descartados", cont["descartados"])
            faltam = max(0, int(quantidade) - cont["aprovados"])
            cnt_meta.metric("🎯 Faltam", faltam)
        except Exception:
            pass

    from pipeline import gerar_lista
    resultado = gerar_lista(
        lista_id=lista_id,
        quantidade_desejada=int(quantidade),
        log_cb=log_cb,
    )

    aprov = resultado.get("aprovados", 0)
    desc  = resultado.get("descartados", 0)
    stat  = resultado.get("status", "")

    cnt_aprovados.metric("✅ Aprovados", aprov)
    cnt_descartados.metric("❌ Descartados", desc)
    cnt_meta.metric("🎯 Faltam", max(0, int(quantidade) - aprov))
    cnt_status.metric("📊 Status",
                      "Concluída" if stat == "concluida" else "Esgotada")

    if stat == "concluida":
        st.success(f"✅ Lista concluída com {aprov} leads aprovados!")
    elif stat == "esgotada":
        st.warning(
            f"⚠️ Esgotamento declarado: {aprov}/{int(quantidade)} aprovados. "
            "Não há mais candidatos disponíveis no momento."
        )
    else:
        st.error(f"Erro: {resultado.get('erro', 'desconhecido')}")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📋 Ver Lista", type="primary", use_container_width=True):
            st.session_state.lista_detalhe_id = lista_id
            st.session_state.pagina = "listas"
            st.rerun()
    with col_b:
        if st.button("➕ Nova Lista", use_container_width=True):
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# LISTAS
# ══════════════════════════════════════════════════════════════════════════════

def pagina_listas():
    if st.session_state.lista_detalhe_id:
        pagina_detalhe(st.session_state.lista_detalhe_id)
        return

    st.title("📋 Listas Geradas")
    listas = db.listar_listas()
    if not listas:
        st.info("Nenhuma lista criada ainda.")
        return

    STATUS = {
        "concluida":   ("Concluída",  "#10B981"),
        "esgotada":    ("Esgotada",   "#F59E0B"),
        "gerando":     ("Gerando",    "#3B82F6"),
        "erro":        ("Erro",       "#EF4444"),
        "criando":     ("Criando",    "#6B7280"),
    }

    for lista in listas:
        status = lista.get("status", "")
        label, cor = STATUS.get(status, (status, "#6B7280"))
        col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
        with col1:
            st.markdown(
                f"**{lista['nome']}**  \n"
                f"<small style='color:#8b949e'>{lista['created_at'][:10]}</small>  "
                f'<span class="badge" style="background:{cor}20;color:{cor};'
                f'border:1px solid {cor}50">{label}</span>',
                unsafe_allow_html=True)
        with col2:
            st.metric("Pedidas", lista.get("requested_quantity", 0))
        with col3:
            st.metric("✅ Aprov.", lista.get("approved_quantity", 0))
        with col4:
            st.metric("❌ Desc.", lista.get("discarded_quantity", 0))
        with col5:
            if st.button("Abrir", key=f"open_{lista['id']}",
                         use_container_width=True):
                st.session_state.lista_detalhe_id = lista["id"]
                st.rerun()
        st.markdown("---")


def pagina_detalhe(lista_id: int):
    lista = db.get_lista(lista_id)
    if not lista:
        st.error("Lista não encontrada.")
        st.session_state.lista_detalhe_id = None
        return

    if st.button("← Voltar"):
        st.session_state.lista_detalhe_id = None
        st.rerun()

    st.title(f"📋 {lista['nome']}")
    cont = db.contar_leads(lista_id)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("✅ Aprovados",   cont["aprovados"])
    c2.metric("❌ Descartados", cont["descartados"])
    c3.metric("📌 Pedidos",     lista.get("requested_quantity", 0))
    c4.metric("📊 Status",      lista.get("status", "").title())

    # Botão para marcar leads como abordados
    st.markdown("---")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("📤 Marcar todos aprovados como Abordados",
                     help="Move leads para a base global de abordadas"):
            leads = db.get_leads_da_lista(lista_id, apenas_aprovados=True)
            for l in leads:
                db.inserir_abordada(
                    nome=l["nome_imobiliaria"],
                    cidade=l.get("cidade",""),
                    lista_id=lista_id,
                    cnpj=l.get("cnpj"),
                    instagram_handle=l.get("instagram_handle"),
                    site_url=l.get("site_url"),
                )
            st.success(f"{len(leads)} leads marcados como abordados.")
            st.rerun()

    st.markdown("---")
    tab_aprov, tab_desc, tab_export = st.tabs([
        f"✅ Aprovados ({cont['aprovados']})",
        f"❌ Descartados ({cont['descartados']})",
        "📥 Exportar",
    ])

    with tab_aprov:
        _tab_aprovados(lista_id)
    with tab_desc:
        _tab_descartados(lista_id)
    with tab_export:
        _tab_exportar(lista_id, lista["nome"])


def _tab_aprovados(lista_id: int):
    leads = db.get_leads_da_lista(lista_id, apenas_aprovados=True)
    if not leads:
        st.info("Nenhum lead aprovado.")
        return

    col1, col2 = st.columns(2)
    with col1:
        f_cidade = st.text_input("Filtrar cidade", key=f"fc_{lista_id}")
    with col2:
        f_anuncia = st.selectbox("Anuncia", ["Todos", "Sim", "Não"],
                                 key=f"fa_{lista_id}")

    filtrados = leads
    if f_cidade:
        filtrados = [l for l in filtrados
                     if f_cidade.lower() in (l.get("cidade") or "").lower()]
    if f_anuncia == "Sim":
        filtrados = [l for l in filtrados
                     if l.get("advertise_meta") or l.get("advertise_google")]
    elif f_anuncia == "Não":
        filtrados = [l for l in filtrados
                     if not l.get("advertise_meta") and not l.get("advertise_google")]

    st.markdown(f"**{len(filtrados)} leads**")
    df = pd.DataFrame([_formatar_lead(l) for l in filtrados])
    st.dataframe(df, use_container_width=True, hide_index=True)


def _tab_descartados(lista_id: int):
    leads = db.get_leads_da_lista(lista_id)
    descartados = [l for l in leads if not l.get("approved")]
    if not descartados:
        st.info("Nenhum lead descartado.")
        return

    MOTIVOS_PT = {
        "sem_site":             "Sem site",
        "sem_instagram":        "Sem Instagram",
        "poucos seguidores":    "Poucos seguidores",
        "poucos posts":         "Poucos posts",
        "inativo":              "Instagram inativo",
        "perfil privado":       "Perfil privado",
        "perfil não existe":    "Perfil não existe",
        "nome da empresa não encontrado no perfil": "Nome ausente no perfil",
        "data do último post não detectada": "Revisar manualmente",
    }

    por_motivo: dict = {}
    for l in descartados:
        m = l.get("discard_reason") or "outro"
        # Simplifica o motivo para agrupamento
        m_simples = next((v for k, v in MOTIVOS_PT.items() if k in m), m)
        por_motivo.setdefault(m_simples, []).append(l)

    for motivo, grupo in sorted(por_motivo.items(), key=lambda x: -len(x[1])):
        with st.expander(f"**{motivo}** — {len(grupo)}"):
            for l in grupo:
                st.markdown(
                    f"- **{l['nome_imobiliaria']}** ({l.get('cidade','—')})"
                    f" | {l.get('discard_reason','')}"
                )


def _tab_exportar(lista_id: int, nome_lista: str):
    st.subheader("Exportar")
    nome_arq = nome_lista.replace(" ", "_").lower()
    leads    = db.get_leads_da_lista(lista_id, apenas_aprovados=True)
    if not leads:
        st.warning("Sem leads aprovados para exportar.")
        return

    df = pd.DataFrame([_formatar_lead(l) for l in leads])

    col1, col2 = st.columns(2)
    with col1:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="Leads")
        buf.seek(0)
        st.download_button("⬇️ Excel", data=buf,
                           file_name=f"{nome_arq}_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col2:
        csv = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ CSV", data=csv,
                           file_name=f"{nome_arq}_{date.today()}.csv",
                           mime="text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# BASES GLOBAIS
# ══════════════════════════════════════════════════════════════════════════════

def pagina_bases():
    st.title("🗃️ Bases Globais")
    tab_repr, tab_abord = st.tabs(["🚫 Reprovadas", "📤 Abordadas"])

    with tab_repr:
        st.markdown("Empresas que não atenderam os critérios mínimos. "
                    "Não entram em novas listas.")
        reprov = db.get_reprovadas(limit=500)
        if not reprov:
            st.info("Base vazia.")
        else:
            st.markdown(f"**{len(reprov)} empresas**")
            df = pd.DataFrame([{
                "Nome":   r["nome_imobiliaria"],
                "Cidade": r.get("cidade",""),
                "CNPJ":   r.get("cnpj","") or "—",
                "Motivo": r.get("motivo",""),
                "Data":   r.get("created_at","")[:10],
            } for r in reprov])
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tab_abord:
        st.markdown("Empresas já prospectadas comercialmente. "
                    "Não entram em novas listas.")
        abord = db.get_abordadas(limit=500)
        if not abord:
            st.info("Base vazia.")
        else:
            st.markdown(f"**{len(abord)} empresas**")
            df2 = pd.DataFrame([{
                "Nome":        a["nome_imobiliaria"],
                "Cidade":      a.get("cidade",""),
                "CNPJ":        a.get("cnpj","") or "—",
                "Instagram":   a.get("instagram_handle","") or "—",
                "Abordada em": a.get("data_abordagem","") or "—",
            } for a in abord])
            st.dataframe(df2, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# EXPORTAR
# ══════════════════════════════════════════════════════════════════════════════

def pagina_exportar():
    st.title("📥 Exportar")
    listas = [l for l in db.listar_listas()
              if l["status"] in ("concluida","esgotada")]
    if not listas:
        st.info("Nenhuma lista disponível.")
        return
    escolha = st.selectbox(
        "Lista",
        options=[l["id"] for l in listas],
        format_func=lambda x: next(l["nome"] for l in listas if l["id"] == x),
    )
    if escolha:
        pagina_detalhe(escolha)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ══════════════════════════════════════════════════════════════════════════════

def pagina_config():
    st.title("⚙️ Configurações")
    from config import MAX_META_ADS, MAX_GOOGLE_ADS, MAX_WEEKS_SINCE_LAST_POST, MIN_CITY_POPULATION
    from agents.enrichment_agent import MIN_SEGUIDORES, MIN_POSTS, MAX_SEMANAS_INATIVO
    from pipeline import UFS_VAZIAS_PARA_ESGOTAR

    st.subheader("Critérios de aprovação")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Mín. seguidores Instagram", MIN_SEGUIDORES)
        st.metric("Mín. posts Instagram", MIN_POSTS)
    with c2:
        st.metric("Máx. semanas inativo", MAX_SEMANAS_INATIVO)
        st.metric("Pop. mínima cidade", f"{MIN_CITY_POPULATION:,}")
    with c3:
        st.metric("UFs vazias p/ esgotamento", UFS_VAZIAS_PARA_ESGOTAR)

    st.info("Para alterar qualquer critério, edite `config.py` ou "
            "`agents/enrichment_agent.py` ou `pipeline.py` diretamente.")
    st.markdown("---")
    st.subheader("🗄️ Banco")
    st.code(str(db.DB_PATH), language="bash")
    st.markdown("Auditoria: [DB Browser for SQLite](https://sqlitebrowser.org/).")
    st.markdown("---")
    st.subheader("⚠️ Resetar banco")
    if st.checkbox("Confirmar reset (apaga TUDO, incluindo reprovadas e abordadas)"):
        if st.button("🔴 Resetar", type="secondary"):
            import os
            if db.DB_PATH.exists():
                os.remove(db.DB_PATH)
            db.init_db()
            st.success("Banco resetado.")
            st.rerun()


# ── Roteador ──────────────────────────────────────────────────────────────────
pagina = st.session_state.pagina
if pagina == "criar_lista":
    pagina_criar_lista()
elif pagina == "listas":
    pagina_listas()
elif pagina == "bases":
    pagina_bases()
elif pagina == "exportar":
    pagina_exportar()
elif pagina == "config":
    pagina_config()
