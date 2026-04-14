"""
config.py — Configurações centralizadas do SDR Imobiliário.
Todos os thresholds de negócio ficam aqui.
"""

import os
from pathlib import Path

# ─── META ADS ─────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN = os.environ.get("META_TOKEN", "")

# ─── GERAL ────────────────────────────────────────────────────────────────────
APP_TITLE = "SDR Imobiliário"
APP_ICON  = "🏢"

# ─── REGRAS DE NEGÓCIO (configuráveis) ────────────────────────────────────────
MIN_CITY_POPULATION          = 100_000
MAX_WEEKS_SINCE_LAST_POST    = 8
MAX_META_ADS                 = 8
MAX_GOOGLE_ADS               = 15

# Aliases legados
LIMITE_ADS_META          = MAX_META_ADS
LIMITE_ADS_GOOGLE        = MAX_GOOGLE_ADS
LIMITE_INSTAGRAM_SEMANAS = MAX_WEEKS_SINCE_LAST_POST

# ─── DESCOBERTA ───────────────────────────────────────────────────────────────
DISCOVERY_SEARCH_TERMS = [
    "imobiliária {cidade}",
    "imobiliaria {cidade}",
    "imóveis {cidade}",
    "imoveis {cidade}",
    "negócios imobiliários {cidade}",
    "corretor imóveis {cidade}",
]
MAX_CANDIDATES_PER_CITY   = 40
GENERATION_BATCH_SIZE     = 5

# ─── VALIDAÇÃO DE CIDADE ──────────────────────────────────────────────────────
ENABLE_CITY_POPULATION_VALIDATION = True
ALLOW_CITY_IF_POPULATION_UNKNOWN  = True

# ─── PORTAIS ─────────────────────────────────────────────────────────────────
ENABLE_PORTALS_CHECK = True

# ─── TIMEOUTS ─────────────────────────────────────────────────────────────────
REQUEST_TIMEOUTS = {
    "default": 12,
    "instagram": 15,
    "google": 15,
    "cnpj": 15,
    "whatsapp": 10,
    "site": 12,
}

# ─── CAMINHOS ─────────────────────────────────────────────────────────────────
EXPORT_DIR = Path(__file__).parent / "exports"
EXPORT_DIR.mkdir(exist_ok=True)

# ─── CIDADES ≥ 100 MIL HABITANTES ────────────────────────────────────────────
CIDADES_POPULACAO: dict = {
    "São Paulo": 11451245, "Rio de Janeiro": 6211223, "Brasília": 3055149,
    "Salvador": 2886698, "Fortaleza": 2703391, "Belo Horizonte": 2315560,
    "Manaus": 2063689, "Curitiba": 1948626, "Recife": 1488920, "Goiânia": 1536097,
    "Belém": 1499641, "Porto Alegre": 1332570, "Guarulhos": 1394123,
    "Campinas": 1213792, "São Luís": 1101884, "São Gonçalo": 1044058,
    "Maceió": 1012382, "Duque de Caxias": 924624, "Natal": 890480,
    "Teresina": 868075, "Campo Grande": 906092, "Nova Iguaçu": 820636,
    "São Bernardo do Campo": 844483, "João Pessoa": 817511, "Santo André": 721136,
    "Osasco": 696850, "Jaboatão dos Guararapes": 702621, "Contagem": 668841,
    "Ribeirão Preto": 711825, "Uberlândia": 699097, "Sorocaba": 696196,
    "Aracaju": 664908, "Feira de Santana": 621083, "Cuiabá": 623614,
    "Juiz de Fora": 563769, "Joinville": 616317, "Aparecida de Goiânia": 590832,
    "Londrina": 575377, "Ananindeua": 535547, "Porto Velho": 539354,
    "Serra": 527240, "Niterói": 502696, "Belford Roxo": 494134,
    "Caxias do Sul": 503247, "Florianópolis": 516524, "São João de Meriti": 460388,
    "Macapá": 503327, "Mogi das Cruzes": 449229, "Santos": 433966,
    "Betim": 444036, "Montes Claros": 413486, "Mauá": 468159,
    "São José dos Campos": 729737, "Carapicuíba": 398985, "Olinda": 390128,
    "Diadema": 386647, "Campina Grande": 411807, "Jundiaí": 422161,
    "Piracicaba": 408433, "Cariacica": 383917, "Bauru": 374272,
    "Vila Velha": 501325, "Canoas": 349896, "São José do Rio Preto": 463690,
    "Pelotas": 328275, "Governador Valadares": 279695, "Caucaia": 362370,
    "Vitória": 365855, "Franca": 356700, "Maringá": 430157,
    "Anápolis": 391772, "São Vicente": 355542, "Palmas": 306296,
    "Caruaru": 361118, "Vitória da Conquista": 341597, "Cascavel": 341473,
    "Limeira": 306594, "São Carlos": 254484, "Imperatriz": 258736,
    "Ribeirão das Neves": 329285, "Blumenau": 352984, "Santarém": 304589,
    "Petrópolis": 305687, "Camaçari": 300372, "Volta Redonda": 272847,
    "Suzano": 290892, "Guarujá": 310441, "Macaé": 251631,
    "Novo Hamburgo": 238940, "São Leopoldo": 229468, "Petrolina": 343865,
    "Itabuna": 206220, "Americana": 241976, "Taboão da Serra": 278984,
    "Taubaté": 318733, "Barreiras": 159556, "Ilhéus": 155574,
    "Marabá": 280081, "Mossoró": 295640, "Cabo Frio": 233404,
    "Foz do Iguaçu": 256772, "Ribeirão Pires": 118229,
}

CIDADES_100K: list = sorted(CIDADES_POPULACAO.keys())


def validar_populacao_cidade(cidade: str) -> tuple:
    """Retorna (valida: bool, populacao: int|None)."""
    if not ENABLE_CITY_POPULATION_VALIDATION:
        return True, None
    cidade_norm = cidade.strip().lower()
    for nome, pop in CIDADES_POPULACAO.items():
        if nome.lower() == cidade_norm:
            return pop >= MIN_CITY_POPULATION, pop
    return ALLOW_CITY_IF_POPULATION_UNKNOWN, None
