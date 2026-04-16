"""
config.py — Configurações centralizadas do SDR Imobiliário v12.
TODOS os thresholds de negócio ficam aqui.
Os módulos importam daqui — nunca definem valores duplicados.
"""

import os
from pathlib import Path

# ─── META ADS ─────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN = os.environ.get("META_TOKEN", "")

# ─── GERAL ────────────────────────────────────────────────────────────────────
APP_TITLE = "SDR Imobiliário"
APP_ICON  = "🏢"

# ─── REGRAS DE NEGÓCIO ────────────────────────────────────────────────────────
MIN_CITY_POPULATION       = 100_000   # habitantes mínimos para considerar cidade
MIN_SEGUIDORES_IG         = 500       # seguidores mínimos no Instagram
MIN_POSTS_IG              = 20        # posts mínimos no Instagram
MAX_SEMANAS_SEM_POST      = 8         # semanas máximas desde o último post
MAX_GOOGLE_ADS            = 10        # limite de anúncios Google (>= descarta)

# ─── ESGOTAMENTO DE PIPELINE ─────────────────────────────────────────────────
UFS_VAZIAS_PARA_ESGOTAR   = 3         # UFs consecutivas sem candidato → para

# ─── TIMEOUTS (segundos) ─────────────────────────────────────────────────────
TIMEOUT_HTTP              = 10        # requests padrão
TIMEOUT_HTTP_RETRY        = 16        # requests após timeout
TIMEOUT_SELENIUM_SITE     = 6         # Google Ads Transparency
TIMEOUT_DDG               = 4         # espera DDG carregar

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
