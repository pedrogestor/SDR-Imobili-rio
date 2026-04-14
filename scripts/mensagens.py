"""
scripts/mensagens.py — Templates de abordagem e follow-ups.
Todos os textos são exatamente os do arquivo original do Pedro,
com placeholders padronizados para substituição automática.
"""

# ─── ABORDAGENS INICIAIS ──────────────────────────────────────────────────────

ABORDAGEM_A = """\
Oi, tudo bem?

Me chamo Pedro, sou gestor de tráfego especializado no setor imobiliário e acompanho o trabalho de vocês em {cidade}.

Notei que, apesar do ótimo posicionamento e autoridade visual que vocês têm no Instagram, a {nome} ainda não está dominando as buscas de intenção no Google aqui na região.

Tomei a iniciativa de estruturar um material de 8 páginas chamado *Domínio de Intenção*, focado exclusivamente em como vocês podem capturar leads que já estão prontos para comprar, com um custo muito menor que os portais.

Posso enviar o PDF por aqui para o {responsavel} ou responsável pelo marketing? Acredito que a estratégia de "Correspondência Cirúrgica" que apresento vai abrir os olhos de vocês para uma oportunidade gigante."""

ABORDAGEM_B = """\
Oi, tudo bem?

Me chamo Pedro, sou gestor de tráfego imobiliário e acompanho os anúncios de vocês no Google aqui em {cidade}.

Notei que vocês já investem na rede de pesquisa, o que é excelente. No entanto, identifiquei alguns gargalos na *Coerência dos Elementos* (entre o anúncio e a página de destino) que podem estar fazendo vocês pagarem muito caro por cada lead.

Estruturei um material de 8 páginas focado na {nome} mostrando como otimizar essa jornada e reduzir o CPL usando silos de intenção.

Posso enviar o PDF para o {responsavel} ou o responsável pelo marketing? Acredito que vai ajudar vocês a terem muito mais eficiência no que já estão investindo."""

ABORDAGEM_C = """\
Oi, tudo bem?

Me chamo Pedro, sou gestor de tráfego imobiliário e acompanho o trabalho de vocês em {cidade}.

Notei que a {nome} já faz um trabalho forte de anúncios no Instagram e Facebook, o que mostra que vocês já entendem o poder do tráfego pago.

No entanto, ao analisar o cenário local, vi que existe uma oportunidade de "cercar" o cliente no Google que vocês ainda não estão explorando. É o lead que já está com a intenção de compra no topo, diferente do Instagram onde ele está apenas navegando.

Tomei a iniciativa de estruturar um material de 8 páginas chamado *Domínio de Intenção*, focado em como integrar o Google Ads à estratégia que vocês já fazem no Meta para dominar o mercado local.

Posso enviar o PDF por aqui para o {responsavel} ou o responsável? Acredito que vai dar uma visão bem diferente sobre escala para vocês."""

ABORDAGENS = {"A": ABORDAGEM_A, "B": ABORDAGEM_B, "C": ABORDAGEM_C}

ABORDAGENS_LABELS = {
    "A": "Padrão (sem anúncios)",
    "B": "Já anuncia no Google",
    "C": "Já anuncia no Meta",
}

# ─── FOLLOW-UPS: NÃO RESPONDEU ───────────────────────────────────────────────

FOLLOWUP_NAO_RESPONDEU = {
    1: """\
Oi {responsavel}, tudo bem?

Eu estou com o material *Domínio de Intenção* pronto para a {nome}, mas percebi que talvez você não tenha visto a mensagem inicial.

Se tiver interesse em ver como capturar leads exclusivos no Google, posso te enviar o material por aqui! Só me avisar.""",

    2: """\
Olá {responsavel}, tudo certo?

Vi que você ainda não respondeu sobre o material do Google Ads que eu tenho aqui, e acredito que ele pode ser bem útil para reduzir a dependência de portais da {nome}.

Se quiser, posso te enviar o material agora mesmo. É só me dar um sinal!""",

    3: """\
Oi {responsavel}, tudo bem?

Eu fiz esse material focado na {nome} e vi algumas oportunidades no Google que podem gerar leads muito mais qualificados do que os que vocês recebem hoje.

Se você tiver interesse, posso te enviar agora mesmo.

Podemos agendar um horário rápido para conversar sobre como implementar isso?""",

    4: """\
Olá {responsavel}, percebo que você ainda não teve a chance de responder sobre o material que preparei.

Entendo que a rotina é corrida, mas se quiser aproveitar essa oportunidade de baixar seu CPL, estou à disposição para enviar o material gratuitamente.

Se achar que não é o momento, me avise, e eu sigo com outras imobiliárias da região. Fico aguardando!""",

    5: """\
Oi, tudo bem?

Te procurei para enviar a estratégia de Google Ads da {nome} há alguns dias e não recebi retorno.

Vou assumir que esse material não é uma prioridade para vocês agora, então não quero tomar mais seu tempo.

Se decidir profissionalizar o tráfego no Google mais tarde, fico à disposição. Sucesso por aí!""",
}

# ─── FOLLOW-UPS: PEDIU MATERIAL ──────────────────────────────────────────────

FOLLOWUP_PEDIU_MATERIAL = {
    1: """\
Olá {responsavel}, tudo bem? Eu enviei o material *Domínio de Intenção* como prometido!

Gostaria de saber se você conseguiu chegar na *Página 4*?

Lá eu falo sobre a "Coerência dos Elementos", que é onde 95% das imobiliárias erram e jogam dinheiro fora no Google. A análise fez sentido para você?""",

    2: """\
Oi {responsavel}, como você está? Queria saber se você teve a oportunidade de ler o material completo.

O que achou da comparação de custos na *Página 3* entre os Portais e o nosso método?

Quais daqueles pontos você acha que seriam mais urgentes de aplicar na {nome} hoje?""",

    3: """\
Olá {responsavel}, tudo certo? Gostaria de saber se você viu a estratégia e se tem algum ponto específico que chamou sua atenção.

Eu acredito que a aplicação daquela lógica de "Silos de Intenção" pode gerar resultados bem interessantes para vocês já no primeiro mês.

Podemos agendar uma reunião de 20 minutos para eu te mostrar como isso funcionaria na prática?""",

    4: """\
Oi {responsavel}, tudo bem? Eu queria reforçar o valor da *Página 7* do material, onde detalhei a "Matemática do Resultado".

Acredito que com aquela projeção de CPL e ROAS, a {nome} teria uma previsibilidade de vendas muito maior.

Estou disponível para discutir isso em uma reunião estratégica. Quando podemos agendar?""",

    5: """\
Olá {responsavel}, tudo bem? Percebo que você ainda não deu retorno sobre os pontos que discutimos no material.

Vou assumir que a estratégia não teve tanto valor para você agora e não quero ser inconveniente.

Se precisar de uma nova análise ou decidir avançar com o Google Ads no futuro, minha agenda está aberta. Fico à disposição!""",
}

# ─── FUNÇÕES ─────────────────────────────────────────────────────────────────

def gerar_abordagem(tipo: str, nome: str, cidade: str, responsavel: str = None) -> str:
    """Gera o texto da abordagem inicial preenchendo os placeholders."""
    template = ABORDAGENS.get(tipo, ABORDAGEM_A)
    resp = responsavel or "responsável"
    return template.format(
        nome=nome,
        cidade=cidade,
        responsavel=resp,
    )


def gerar_followup(num: int, pediu_material: bool,
                   nome: str, responsavel: str = None) -> str:
    """Gera o texto do follow-up N."""
    resp = responsavel or "tudo bem"
    banco = FOLLOWUP_PEDIU_MATERIAL if pediu_material else FOLLOWUP_NAO_RESPONDEU
    template = banco.get(num, "")
    if not template:
        return ""
    return template.format(nome=nome, responsavel=resp)


def dias_para_proximo_followup(num_atual: int) -> int:
    """Retorna quantos dias aguardar antes do próximo follow-up."""
    mapa = {0: 3, 1: 3, 2: 3, 3: 4, 4: 4}  # num_atual → dias de espera
    return mapa.get(num_atual, 5)


STATUS_LABELS = {
    "pendente": "⏳ Pendente",
    "abordagem_enviada": "📤 Abordagem enviada",
    "pediu_material": "📄 Pediu material",
    "em_followup": "🔄 Em follow-up",
    "reuniao_agendada": "📅 Reunião agendada",
    "descartado": "🚫 Descartado",
    "fechado": "✅ Fechado",
}

STATUS_CORES = {
    "pendente": "#6B7280",
    "abordagem_enviada": "#3B82F6",
    "pediu_material": "#F59E0B",
    "em_followup": "#8B5CF6",
    "reuniao_agendada": "#10B981",
    "descartado": "#EF4444",
    "fechado": "#059669",
}
