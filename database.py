"""
database.py — Schema e acesso ao SQLite.
Tabelas: listas, leads_enriquecidos, reprovadas, abordadas, excecoes.
"""

import sqlite3, json, re, unicodedata
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "prospeccao.db"


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS listas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            nome                TEXT NOT NULL,
            status              TEXT DEFAULT 'criando',
            requested_quantity  INTEGER NOT NULL DEFAULT 0,
            approved_quantity   INTEGER DEFAULT 0,
            discarded_quantity  INTEGER DEFAULT 0,
            criteria_json       TEXT DEFAULT '{}',
            source_cities_json  TEXT DEFAULT '[]',
            created_at          TEXT DEFAULT (datetime('now','localtime')),
            updated_at          TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS leads_enriquecidos (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            lista_id                INTEGER NOT NULL REFERENCES listas(id),
            nome_imobiliaria        TEXT NOT NULL,
            cidade                  TEXT,
            estado                  TEXT,
            instagram_url           TEXT,
            instagram_handle        TEXT,
            instagram_exists        INTEGER DEFAULT 0,
            instagram_private       INTEGER DEFAULT 0,
            instagram_seguidores    INTEGER,
            instagram_num_posts     INTEGER,
            last_post_date          TEXT,
            weeks_since_last_post   INTEGER,
            instagram_active        INTEGER DEFAULT 0,
            site_url                TEXT,
            site_status             INTEGER,
            site_ok                 INTEGER DEFAULT 0,
            meta_ads_count          INTEGER DEFAULT 0,
            google_ads_count        INTEGER DEFAULT 0,
            advertise_meta          INTEGER DEFAULT 0,
            advertise_google        INTEGER DEFAULT 0,
            dominant_channel        TEXT,
            cnpj                    TEXT,
            razao_social            TEXT,
            responsaveis_json       TEXT DEFAULT '[]',
            responsavel_principal   TEXT,
            email                   TEXT,
            telefone_raw            TEXT,
            whatsapp_number         TEXT,
            whatsapp_valid          INTEGER DEFAULT 0,
            whatsapp_source         TEXT,
            whatsapp_link           TEXT,
            portals_json            TEXT DEFAULT '[]',
            discard_reason          TEXT,
            approved                INTEGER DEFAULT 0,
            raw_debug_json          TEXT DEFAULT '{}',
            created_at              TEXT DEFAULT (datetime('now','localtime')),
            updated_at              TEXT DEFAULT (datetime('now','localtime'))
        );

        -- ── REPROVADAS (global — persiste entre listas) ────────────────────
        -- Empresas que não atendem os critérios mínimos de prospecção.
        -- Nunca voltam a ser processadas.
        CREATE TABLE IF NOT EXISTS reprovadas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_imobiliaria    TEXT NOT NULL,
            cidade              TEXT,
            cnpj                TEXT,
            instagram_handle    TEXT,
            site_dominio        TEXT,
            motivo              TEXT NOT NULL,
            origem_lista_id     INTEGER REFERENCES listas(id),
            created_at          TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_repr_cnpj    ON reprovadas(cnpj);
        CREATE INDEX IF NOT EXISTS idx_repr_ig      ON reprovadas(instagram_handle);
        CREATE INDEX IF NOT EXISTS idx_repr_dominio ON reprovadas(site_dominio);

        -- ── ABORDADAS (global — persiste entre listas) ─────────────────────
        -- Empresas que já foram prospectadas comercialmente.
        -- Não entram em listas futuras.
        CREATE TABLE IF NOT EXISTS abordadas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_imobiliaria    TEXT NOT NULL,
            cidade              TEXT,
            cnpj                TEXT,
            instagram_handle    TEXT,
            site_dominio        TEXT,
            data_abordagem      TEXT,
            status_abordagem    TEXT DEFAULT 'abordada',
            origem_lista_id     INTEGER REFERENCES listas(id),
            created_at          TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_abord_cnpj ON abordadas(cnpj);
        CREATE INDEX IF NOT EXISTS idx_abord_ig   ON abordadas(instagram_handle);

        -- ── TABELAS LEGADAS ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS imobiliarias (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL,
            cidade TEXT, uf TEXT, instagram_url TEXT, instagram_ativo INTEGER DEFAULT 0,
            instagram_ultimo_post TEXT, site_url TEXT, site_funcional INTEGER DEFAULT 0,
            anuncia_meta INTEGER DEFAULT 0, qtd_anuncios_meta INTEGER DEFAULT 0,
            anuncia_google INTEGER DEFAULT 0, qtd_anuncios_google INTEGER DEFAULT 0,
            portais TEXT DEFAULT '[]', cnpj TEXT, responsavel TEXT,
            email TEXT, telefones TEXT DEFAULT '[]', whatsapp_link TEXT,
            whatsapp_validado INTEGER DEFAULT 0, padrao_imoveis TEXT,
            material_valor TEXT DEFAULT 'GOOGLE', abordagem_tipo TEXT,
            descartado INTEGER DEFAULT 0, motivo_descarte TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS prospeccoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            imobiliaria_id INTEGER NOT NULL REFERENCES imobiliarias(id),
            status TEXT DEFAULT 'pendente', follow_up_num INTEGER DEFAULT 0,
            follow_up_tipo TEXT, data_contato TEXT, data_proximo_followup TEXT,
            pediu_material INTEGER DEFAULT 0, observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS log_acoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            imobiliaria_id INTEGER REFERENCES imobiliarias(id),
            prospeccao_id INTEGER REFERENCES prospeccoes(id),
            acao TEXT NOT NULL, detalhe TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_imob_cidade    ON imobiliarias(cidade);
        CREATE INDEX IF NOT EXISTS idx_imob_descartado ON imobiliarias(descartado);
        CREATE INDEX IF NOT EXISTS idx_leads_lista    ON leads_enriquecidos(lista_id);
        CREATE INDEX IF NOT EXISTS idx_leads_approved ON leads_enriquecidos(approved);
        CREATE INDEX IF NOT EXISTS idx_listas_status  ON listas(status);
        """)


# ══════════════════════════════════════════════════════════════════════════════
# UTILITÁRIOS DE NORMALIZAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", s)

def _limpar_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")

def _dominio(url: str) -> str | None:
    from urllib.parse import urlparse
    try:
        d = urlparse(url if url.startswith("http") else "https://" + url).netloc
        return d.lower().replace("www.", "") or None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# VERIFICAÇÃO DE ELEGIBILIDADE (consulta todos os bancos de exclusão)
# ══════════════════════════════════════════════════════════════════════════════

def checar_inelegivel(
    nome: str, cidade: str,
    cnpj: str = None,
    instagram_handle: str = None,
    site_url: str = None,
    lista_id: int = None,
) -> tuple[bool, str]:
    """
    Verifica se a empresa está em qualquer banco de exclusão.
    Retorna (inelegivel: bool, motivo: str).
    Chaves de dedup: CNPJ, nome_norm+cidade, instagram_handle, domínio do site.
    """
    cnpj_limpo     = _limpar_cnpj(cnpj) if cnpj else None
    handle_lower   = instagram_handle.lower() if instagram_handle else None
    dominio_site   = _dominio(site_url) if site_url else None
    nome_norm      = _norm(nome)
    cidade_norm    = _norm(cidade)

    with get_conn() as conn:

        # 1. Verifica em reprovadas
        for tabela in ("reprovadas", "abordadas"):
            if cnpj_limpo:
                r = conn.execute(
                    f"SELECT 1 FROM {tabela} WHERE cnpj=? LIMIT 1",
                    (cnpj_limpo,)).fetchone()
                if r:
                    return True, f"cnpj em {tabela}"

            if handle_lower:
                r = conn.execute(
                    f"SELECT 1 FROM {tabela} WHERE instagram_handle=? LIMIT 1",
                    (handle_lower,)).fetchone()
                if r:
                    return True, f"instagram em {tabela}"

            if dominio_site:
                r = conn.execute(
                    f"SELECT 1 FROM {tabela} WHERE site_dominio=? LIMIT 1",
                    (dominio_site,)).fetchone()
                if r:
                    return True, f"site em {tabela}"

            # Nome normalizado + cidade
            rows = conn.execute(
                f"SELECT nome_imobiliaria, cidade FROM {tabela}").fetchall()
            for row in rows:
                if (_norm(row["nome_imobiliaria"]) == nome_norm and
                        _norm(row["cidade"] or "") == cidade_norm):
                    return True, f"nome+cidade em {tabela}"

        # 2. Verifica na lista atual (se fornecida)
        if lista_id:
            if cnpj_limpo:
                r = conn.execute(
                    "SELECT 1 FROM leads_enriquecidos WHERE lista_id=? AND cnpj=? LIMIT 1",
                    (lista_id, cnpj_limpo)).fetchone()
                if r:
                    return True, "cnpj já na lista atual"

            if handle_lower:
                r = conn.execute(
                    "SELECT 1 FROM leads_enriquecidos WHERE lista_id=? AND instagram_handle=? LIMIT 1",
                    (lista_id, handle_lower)).fetchone()
                if r:
                    return True, "instagram já na lista atual"

            rows2 = conn.execute(
                "SELECT nome_imobiliaria, cidade FROM leads_enriquecidos WHERE lista_id=?",
                (lista_id,)).fetchall()
            for row in rows2:
                if (_norm(row["nome_imobiliaria"]) == nome_norm and
                        _norm(row["cidade"] or "") == cidade_norm):
                    return True, "nome+cidade já na lista atual"

    return False, ""


def checar_duplicata(lista_id, nome, cidade,
                     instagram_handle=None, cnpj=None):
    """Compatibilidade com código legado."""
    inelig, _ = checar_inelegivel(nome, cidade, cnpj, instagram_handle,
                                  lista_id=lista_id)
    return inelig


# ══════════════════════════════════════════════════════════════════════════════
# REPROVADAS
# ══════════════════════════════════════════════════════════════════════════════

def inserir_reprovada(nome: str, cidade: str, motivo: str,
                      cnpj: str = None, instagram_handle: str = None,
                      site_url: str = None, lista_id: int = None):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO reprovadas
               (nome_imobiliaria, cidade, cnpj, instagram_handle,
                site_dominio, motivo, origem_lista_id)
               VALUES (?,?,?,?,?,?,?)""",
            (nome, cidade,
             _limpar_cnpj(cnpj) if cnpj else None,
             instagram_handle.lower() if instagram_handle else None,
             _dominio(site_url) if site_url else None,
             motivo, lista_id)
        )


def get_reprovadas(limit: int = 500) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM reprovadas ORDER BY created_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# ABORDADAS
# ══════════════════════════════════════════════════════════════════════════════

def inserir_abordada(nome: str, cidade: str, lista_id: int,
                     cnpj: str = None, instagram_handle: str = None,
                     site_url: str = None, data_abordagem: str = None):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO abordadas
               (nome_imobiliaria, cidade, cnpj, instagram_handle,
                site_dominio, data_abordagem, origem_lista_id)
               VALUES (?,?,?,?,?,?,?)""",
            (nome, cidade,
             _limpar_cnpj(cnpj) if cnpj else None,
             instagram_handle.lower() if instagram_handle else None,
             _dominio(site_url) if site_url else None,
             data_abordagem or datetime.now().strftime("%Y-%m-%d"),
             lista_id)
        )


def get_abordadas(limit: int = 500) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM abordadas ORDER BY created_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# LISTAS
# ══════════════════════════════════════════════════════════════════════════════

def criar_lista(nome: str, qtd: int, cidades: list,
                criteria: dict = None) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO listas
               (nome, status, requested_quantity, source_cities_json, criteria_json)
               VALUES (?, 'criando', ?, ?, ?)""",
            (nome, qtd,
             json.dumps(cidades, ensure_ascii=False),
             json.dumps(criteria or {}, ensure_ascii=False))
        )
        return cur.lastrowid


def atualizar_lista(lista_id: int, dados: dict):
    dados["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sets = ", ".join(f"{k} = ?" for k in dados)
    with get_conn() as conn:
        conn.execute(f"UPDATE listas SET {sets} WHERE id = ?",
                     list(dados.values()) + [lista_id])


def get_lista(lista_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM listas WHERE id=?",
                           (lista_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    for f in ("source_cities_json", "criteria_json"):
        try:
            d[f] = json.loads(d[f] or "[]")
        except Exception:
            d[f] = []
    return d


def listar_listas() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM listas ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["source_cities_json"] = json.loads(
                d.get("source_cities_json") or "[]")
        except Exception:
            d["source_cities_json"] = []
        result.append(d)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# LEADS
# ══════════════════════════════════════════════════════════════════════════════

def inserir_lead(lista_id: int, dados: dict) -> int:
    _json_campos = ("responsaveis_json", "portals_json", "raw_debug_json")
    row = dict(dados)
    row["lista_id"] = lista_id
    for f in _json_campos:
        if isinstance(row.get(f), (list, dict)):
            row[f] = json.dumps(row[f], ensure_ascii=False)
    cols         = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    with get_conn() as conn:
        cur = conn.execute(
            f"INSERT INTO leads_enriquecidos ({cols}) VALUES ({placeholders})",
            list(row.values())
        )
        return cur.lastrowid


def atualizar_lead(lead_id: int, dados: dict):
    dados["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for f in ("responsaveis_json", "portals_json"):
        if isinstance(dados.get(f), list):
            dados[f] = json.dumps(dados[f], ensure_ascii=False)
    if isinstance(dados.get("raw_debug_json"), dict):
        dados["raw_debug_json"] = json.dumps(
            dados["raw_debug_json"], ensure_ascii=False)
    sets = ", ".join(f"{k} = ?" for k in dados)
    with get_conn() as conn:
        conn.execute(f"UPDATE leads_enriquecidos SET {sets} WHERE id = ?",
                     list(dados.values()) + [lead_id])


def get_leads_da_lista(lista_id: int,
                       apenas_aprovados: bool = False) -> list[dict]:
    query  = "SELECT * FROM leads_enriquecidos WHERE lista_id = ?"
    params = [lista_id]
    if apenas_aprovados:
        query += " AND approved = 1"
    query += " ORDER BY created_at ASC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        for f in ("responsaveis_json", "portals_json"):
            try:
                d[f] = json.loads(d[f] or "[]")
            except Exception:
                d[f] = []
        result.append(d)
    return result


def contar_leads(lista_id: int) -> dict:
    with get_conn() as conn:
        total     = conn.execute(
            "SELECT COUNT(*) FROM leads_enriquecidos WHERE lista_id=?",
            (lista_id,)).fetchone()[0]
        aprovados = conn.execute(
            "SELECT COUNT(*) FROM leads_enriquecidos WHERE lista_id=? AND approved=1",
            (lista_id,)).fetchone()[0]
        desc      = conn.execute(
            "SELECT COUNT(*) FROM leads_enriquecidos WHERE lista_id=? AND approved=0",
            (lista_id,)).fetchone()[0]
    return {"total": total, "aprovados": aprovados, "descartados": desc}


# ══════════════════════════════════════════════════════════════════════════════
# EXCEÇÕES (por lista)
# ══════════════════════════════════════════════════════════════════════════════

def init_excecoes():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS excecoes (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            lista_id         INTEGER REFERENCES listas(id),
            lead_id          INTEGER REFERENCES leads_enriquecidos(id),
            nome_imobiliaria TEXT NOT NULL,
            cidade           TEXT,
            motivo           TEXT NOT NULL,
            created_at       TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_exc_lista ON excecoes(lista_id);
        """)


def inserir_excecao(lista_id: int, lead_id: int,
                    nome: str, cidade: str, motivo: str):
    init_excecoes()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO excecoes
               (lista_id, lead_id, nome_imobiliaria, cidade, motivo)
               VALUES (?,?,?,?,?)""",
            (lista_id, lead_id, nome, cidade, motivo)
        )


def get_excecoes_da_lista(lista_id: int) -> list[dict]:
    init_excecoes()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM excecoes WHERE lista_id=? ORDER BY created_at ASC",
            (lista_id,)).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# EXPORTAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

def exportar_lista_df(lista_id: int, modo: str = "operacional"):
    import pandas as pd
    leads = get_leads_da_lista(lista_id)
    if not leads:
        return pd.DataFrame()
    df = pd.DataFrame(leads)
    if modo == "operacional":
        cols = ["nome_imobiliaria", "email", "whatsapp_link",
                "cidade", "instagram_url", "site_url",
                "dominant_channel", "responsavel_principal"]
        cols_exist = [c for c in cols if c in df.columns]
        df = df[cols_exist].rename(columns={
            "nome_imobiliaria":    "Nome da imobiliária",
            "email":               "Email",
            "whatsapp_link":       "WhatsApp",
            "cidade":              "Cidade",
            "instagram_url":       "Instagram",
            "site_url":            "Site",
            "dominant_channel":    "Canal dominante",
            "responsavel_principal": "Responsável",
        })
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════════════════════════════════════

def get_stats() -> dict:
    with get_conn() as conn:
        listas    = conn.execute("SELECT COUNT(*) FROM listas").fetchone()[0]
        aprovados = conn.execute(
            "SELECT COUNT(*) FROM leads_enriquecidos WHERE approved=1"
        ).fetchone()[0]
        desc = conn.execute(
            "SELECT COUNT(*) FROM leads_enriquecidos WHERE approved=0"
        ).fetchone()[0]
        repr_count = conn.execute(
            "SELECT COUNT(*) FROM reprovadas").fetchone()[0]
        abord_count = conn.execute(
            "SELECT COUNT(*) FROM abordadas").fetchone()[0]
    return {
        "listas":           listas,
        "leads_aprovados":  aprovados,
        "leads_descartados": desc,
        "reprovadas":       repr_count,
        "abordadas":        abord_count,
    }
