appointments_clean_columns = [
                            'ID agendamento',
                            'ID cliente',
                            'Unidade do agendamento',
                            'Procedimento',
                            'Status',
                            'Data',
                            'Telefone',
                            'Email',
                            'eh_avaliacao',
                            'eh_agendamento',
                            'eh_comparecimento',
                            'eh_agendado',
                            'eh_falta_ou_cancelado'
                            # 'Dia',
                            # 'Mês',
                            # 'Dia da Semana'
] 
 
avaliacao_procedures = [
                        # pro-corpo:
                        'AVALIAÇÃO TATUAGEM',
                        'AVALIAÇÃO INJETÁVEIS E INVASIVOS',
                        'AVALIAÇÃO ESTÉTICA',
                        'AVALIAÇÃO DEPILAÇÃO',
                        'AVALIAÇÃO MEDICINA ESTÉTICA',
                        
                        # cirurgia:
                        'AVALIAÇÃO MAMOPLASTIA',
                        'AVALIAÇÃO PLÁSTICA DE ABDÔMEN',
                        'AVALIAÇÃO MAMOPLASTIA COM PRÓTESE',
                        'AVALIAÇÃO LIPOASPIRAÇÃO',
                        'AVALIAÇÃO PRÓTESE DE MAMA',
                        'AVALIAÇÃO RINOPLASTIA',
                        'AVALIAÇÃO BLEFAROPLASTIA',
                        'AVALIAÇÃO RITIDOPLASTIA',
                        'AVALIAÇÃO CIRURGIA ÍNTIMA',
                        'SEGUNDA OPINIÃO (AVALIAÇÃO CIRURGIA)',
                        
                        # not in use:
                        #'RETORNO DE AVALIAÇÃO (ENTREGA EXAMES)',
                        #'RETORNO CIRURGIA PLÁSTICA',
]

appointments_api_clean_columns = [
    'ID agendamento',
    'ID cliente',
    'Data',
    'Status',
    'Nome cliente',
    'Email',
    'Telefone',
    # 'Endereço',
    # 'CPF',
    'Fonte de cadastro do cliente',
    'Unidade do agendamento',
    'Procedimento',
    'Grupo do procedimento',
    'Prestador',
    'Grupo da primeira atendente',
    'Observação (mais recente)', # TODO pending from this on...
    'Data de atualização',
    'Atualizado por',
    'Último comentário',
    'Data do último comentário',
    'Usuário do último comentário',
    'Data do primeiro comentário',
    'Primeiro comentário',
    'Antes',
    'Em processo',
    'Depois',
]

