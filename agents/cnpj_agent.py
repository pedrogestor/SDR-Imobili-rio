"""
agents/cnpj_agent.py — Consulta CNPJ via ReceitaWS (gratuito, sem chave).
Busca CNPJ pelo nome + cidade quando necessário.
"""

import re
import time
import requests
from urllib.parse import quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def buscar_cnpj_por_nome(nome_empresa: str, cidade: str) -> str | None:
    """
    Tenta encontrar o CNPJ pesquisando no ReceitaWS por nome fantasia + município.
    Retorna o CNPJ (só números) ou None.
    """
    # Normaliza o nome para busca
    nome_limpo = re.sub(r"[^\w\s]", "", nome_empresa).strip()
    query = quote(f"{nome_limpo} {cidade}")

    try:
        # API de busca por nome do ReceitaWS
        url = f"https://receitaws.com.br/v1/company/search?query={query}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            empresas = data.get("companies", [])
            if empresas:
                return _limpar_cnpj(empresas[0].get("cnpj", ""))
    except Exception:
        pass

    # Fallback: busca no CNPJ.biz via scraping leve
    try:
        url2 = f"https://cnpj.biz/pesquisa/{quote(nome_limpo)}"
        r2 = requests.get(url2, headers=HEADERS, timeout=10)
        if r2.status_code == 200:
            # Procura padrão de CNPJ no HTML: XX.XXX.XXX/XXXX-XX
            matches = re.findall(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", r2.text)
            if matches:
                return _limpar_cnpj(matches[0])
    except Exception:
        pass

    return None


def consultar_cnpj(cnpj: str) -> dict:
    """
    Consulta dados completos de um CNPJ.
    Retorna dict com: razao_social, nome_fantasia, email, telefone,
                      socios, municipio, uf, situacao.
    """
    cnpj_limpo = _limpar_cnpj(cnpj)
    if not cnpj_limpo or len(cnpj_limpo) != 14:
        return {"erro": "CNPJ inválido"}

    # Tenta ReceitaWS primeiro
    resultado = _consultar_receitaws(cnpj_limpo)
    if "erro" not in resultado:
        return resultado

    # Fallback: BrasilAPI
    time.sleep(0.5)
    return _consultar_brasilapi(cnpj_limpo)


def _consultar_receitaws(cnpj: str) -> dict:
    try:
        url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 429:
            time.sleep(3)
            r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {"erro": f"HTTP {r.status_code}"}
        data = r.json()
        if data.get("status") == "ERROR":
            return {"erro": data.get("message", "Erro ReceitaWS")}

        socios = []
        for q in data.get("qsa", []):
            nome = q.get("nome", "").title()
            if nome:
                socios.append(nome)

        telefones = []
        for campo in ("telefone",):
            val = data.get(campo, "")
            if val:
                # Pode vir como "21 3333-4444 / 21 99999-8888"
                for parte in re.split(r"[/,;]", val):
                    t = re.sub(r"\D", "", parte.strip())
                    if t:
                        telefones.append(t)

        return {
            "cnpj": cnpj,
            "razao_social": data.get("nome", ""),
            "nome_fantasia": data.get("fantasia", ""),
            "email": data.get("email", "").lower() or None,
            "telefones": telefones,
            "socios": socios,
            "responsavel": socios[0] if socios else None,
            "municipio": data.get("municipio", ""),
            "uf": data.get("uf", ""),
            "situacao": data.get("situacao", ""),
            "abertura": data.get("abertura", ""),
        }
    except Exception as e:
        return {"erro": str(e)}


def _consultar_brasilapi(cnpj: str) -> dict:
    try:
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {"erro": f"BrasilAPI HTTP {r.status_code}"}
        data = r.json()

        socios = []
        for q in data.get("qsa", []):
            nome = q.get("nome_socio", "").title()
            if nome:
                socios.append(nome)

        telefones = []
        for campo in ("ddd_telefone_1", "ddd_telefone_2"):
            val = data.get(campo, "")
            if val:
                t = re.sub(r"\D", "", val)
                if t:
                    telefones.append(t)

        return {
            "cnpj": cnpj,
            "razao_social": data.get("razao_social", ""),
            "nome_fantasia": data.get("nome_fantasia", ""),
            "email": (data.get("email") or "").lower() or None,
            "telefones": telefones,
            "socios": socios,
            "responsavel": socios[0] if socios else None,
            "municipio": data.get("municipio", ""),
            "uf": data.get("uf", ""),
            "situacao": data.get("descricao_situacao_cadastral", ""),
            "abertura": data.get("data_inicio_atividade", ""),
        }
    except Exception as e:
        return {"erro": str(e)}


def _limpar_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def formatar_cnpj(cnpj: str) -> str:
    c = _limpar_cnpj(cnpj)
    if len(c) == 14:
        return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"
    return cnpj
