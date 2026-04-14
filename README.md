# SDR Imobiliário 🏢

Sistema de prospecção automática para gestores de tráfego no mercado imobiliário.

## O que faz

- **Enriquecimento automático** de imobiliárias: Instagram, site, Meta Ads, Google Ads, CNPJ, WhatsApp, portais
- **Descarte automático** por critérios: Instagram inativo (>8 semanas), muitos anúncios Meta (>8) ou Google (>15)
- **CRM de prospecção**: controla status, follow-ups com datas automáticas
- **Scripts prontos**: abordagem A/B/C + 5 follow-ups (não respondeu / pediu material) preenchidos automaticamente
- **Banco auditável**: SQLite com log completo de todas as ações
- **Exportação**: Excel e CSV no formato da sua planilha original

## Instalação

### Pré-requisitos
- Python 3.10 ou superior
- pip

### Passo a passo

```bash
# 1. Entre na pasta do projeto
cd sdr_imobiliario

# 2. (Recomendado) Crie um ambiente virtual
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Rode o app
streamlit run app.py
```

O app vai abrir automaticamente no navegador em `http://localhost:8501`

## Configuração do Token Meta Ads (recomendado)

Para resultados mais precisos na verificação de anúncios Meta:

1. Acesse [developers.facebook.com](https://developers.facebook.com/)
2. Crie um app tipo **Business**
3. Adicione o produto **Marketing API**
4. Vá em **Tools → Graph API Explorer**
5. Gere um **User Access Token** com permissão `ads_read`
6. Abra `config.py` e cole o token em `META_ACCESS_TOKEN`

Sem o token, o sistema usa scraping da Ads Library (funciona, mas menos preciso).

## Estrutura do Projeto

```
sdr_imobiliario/
├── app.py                  # Interface Streamlit (roda aqui)
├── config.py               # Token Meta e configurações
├── database.py             # Toda lógica SQLite
├── enrichment.py           # Orquestrador dos agentes
├── agents/
│   ├── instagram_agent.py  # Valida perfis Instagram
│   ├── site_agent.py       # Verifica se site está no ar
│   ├── ads_agent.py        # Meta Ads + Google Ads
│   ├── cnpj_agent.py       # ReceitaWS + BrasilAPI
│   ├── whatsapp_agent.py   # Valida números + gera links
│   └── portals_agent.py    # ZAP, Viva Real, OLX, Chaves na Mão, Quinto Andar
├── scripts/
│   └── mensagens.py        # Todos os templates A/B/C + follow-ups
├── data/
│   └── prospeccao.db       # Banco SQLite (auditável)
├── exports/                # Excel/CSV gerados
└── requirements.txt
```

## Banco de Dados

O banco fica em `data/prospeccao.db`. Para auditoria direta:
- Baixe o [DB Browser for SQLite](https://sqlitebrowser.org/) (gratuito)
- Abra o arquivo `.db` para ver todas as tabelas e histórico de ações

### Tabelas principais

| Tabela | Descrição |
|--------|-----------|
| `imobiliarias` | Dados completos de cada lead |
| `prospeccoes` | Status e histórico de prospecção |
| `log_acoes` | Log auditável de tudo que aconteceu |

## Fluxo de uso

1. **Nova Lista** → Adicione imobiliárias (manual ou CSV)
2. O sistema enriquece automaticamente (Instagram → Site → Meta → Google → CNPJ → WhatsApp → Portais)
3. **Leads descartados** automaticamente ficam marcados com motivo
4. **Leads válidos** ficam em "Pendente" — abra cada um para ver o script A/B/C já preenchido
5. Clique no link WhatsApp → envie a mensagem → marque como "Abordagem enviada"
6. O sistema calcula automaticamente a data do próximo follow-up
7. **Follow-ups** → veja todos os que vencem hoje com o script correto pronto

## Critérios de descarte automático

| Critério | Regra |
|----------|-------|
| Instagram inativo | Último post há mais de 8 semanas |
| Instagram inexistente | Perfil não encontrado ou privado |
| Muitos anúncios Meta | Mais de 8 anúncios ativos |
| Muitos anúncios Google | Mais de 15 anúncios ativos |

## Abordagens

| Tipo | Quando usar |
|------|-------------|
| **A — Padrão** | Não anuncia em nenhuma plataforma |
| **B — Google** | Já anuncia no Google Ads |
| **C — Meta** | Já anuncia no Meta (Instagram/Facebook) |

Se anunciar nos dois, prioriza o de maior volume.
