"""
agents/discovery_agent.py — Gerador de candidatos via Minha Receita.
Expõe um gerador que entrega um candidato por vez para o pipeline.
"""

import re, time, unicodedata, requests
from config import CIDADES_POPULACAO, MIN_CITY_POPULATION

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

CNAES_IMOB = [
    "6821801",  # Corretagem compra/venda
    "6821802",  # Corretagem aluguel
    "6810201",  # Compra e venda própria
    "6810202",  # Aluguel próprio
    "6822600",  # Gestão imobiliária
]

UFS = [
    "SP","RJ","MG","BA","PR","RS","PE","CE","PA","MA",
    "SC","GO","AM","ES","PB","RN","MT","MS","PI","AL",
    "DF","SE","RO","TO","AC","AP","RR",
]

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").upper())
    return "".join(c for c in s if not unicodedata.combining(c))

MUNICIPIOS_VALIDOS_NORM: set[str] = {
    _norm(nome) for nome, pop in CIDADES_POPULACAO.items()
    if pop >= MIN_CITY_POPULATION
}

def municipio_valido(municipio: str) -> bool:
    return _norm(municipio) in MUNICIPIOS_VALIDOS_NORM

def buscar_por_uf_cnae(uf: str, cnae: str, limite: int = 100) -> list[dict]:
    url = f"https://minhareceita.org/?cnae_fiscal={cnae}&uf={uf}&limit={limite}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("data", []) if isinstance(data, dict) else data
    except Exception:
        pass
    return []


def gerar_candidatos(vistos_cnpjs: set = None, log_cb=None):
    """
    Gerador que entrega um candidato (dict) por vez.
    Itera UFs → CNAEs → empresas.
    Para quando esgotar todas as combinações.

    Yields: dict com nome, razao_social, cidade, estado, cnpj, email,
                       telefone_1, telefone_2, socios, fonte.

    Também yields mensagens de log como strings prefixadas com 'LOG:'.
    """
    def log(msg):
        if log_cb:
            log_cb(msg)

    if vistos_cnpjs is None:
        vistos_cnpjs = set()

    for uf in UFS:
        log(f"🔍 Consultando UF: {uf}")
        uf_novos = 0

        for cnae in CNAES_IMOB:
            empresas = buscar_por_uf_cnae(uf, cnae)
            time.sleep(0.4)

            for emp in empresas:
                cnpj = emp.get("cnpj", "")
                if not cnpj or cnpj in vistos_cnpjs:
                    continue
                municipio = emp.get("municipio", "")
                if not municipio_valido(municipio):
                    continue
                situacao = emp.get("descricao_situacao_cadastral", "").upper()
                if situacao and situacao != "ATIVA":
                    continue

                vistos_cnpjs.add(cnpj)
                uf_novos += 1

                nome = emp.get("nome_fantasia") or emp.get("razao_social") or ""
                yield {
                    "nome":        nome.strip().title(),
                    "razao_social": emp.get("razao_social","").strip().title(),
                    "cidade":      municipio.strip().title(),
                    "estado":      uf,
                    "cnpj":        cnpj,
                    "email":       emp.get("email") or None,
                    "telefone_1":  emp.get("ddd_telefone_1") or None,
                    "telefone_2":  emp.get("ddd_telefone_2") or None,
                    "socios":      [s.get("nome_socio","")
                                    for s in emp.get("qsa", [])],
                    "fonte":       "minhareceita",
                }

        if uf_novos > 0:
            log(f"   ✅ {uf}: {uf_novos} novos candidatos")
        else:
            log(f"   — {uf}: sem novos candidatos")
