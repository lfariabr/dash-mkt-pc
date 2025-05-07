leadsByUserColumns = [
                    'name', 
                    'messages_count', 
                    'unique_messages_count', 
                    'messages_count_by_status', 
                    'success_rate'
]

agendamento_por_lead_column = [
                            'agd', 'jag'
] 

leadsByUser_display_columns = [
                                'Atendente', 'Unidade', 'Turno', 'Tam',
                                'Leads Puxados', 'Leads Puxados (únicos)',
                                'Agendamentos por lead', 'Conversão'
]

followUpEntries_display_columns_initial_columns = ['name', 'follow_ups_count', 'customer_ids']

followUpEntries_display_columns = [
                                'Consultora de Vendas', 'Unidade', 
                                'Turno', 'Tam', 'Novos Pós-Vendas'
] #, 'ID dos Clientes']

followUpComments_display_columns_initial_columns = [
    'name',
    'comments_count',
    'comments_customer_ids'
]

followUpComments_display_columns = [
    'Consultora de Vendas',
    'Unidade',
    'Turno',
    'Tam',
    'Comentários de Pós-Vendas'
] #, 'ID dos Clientes']

grossSales_display_columns = ['createdBy', 'chargableTotal', 'statusLabel', 'id']