import re

# Função para limpar telefones
def clean_telephone(telefone):
    # Remove todos os caracteres não numéricos e garante que o valor é uma string
    telefone = re.sub(r'[^\d]', '', str(telefone))
    # Remove o prefixo +55, caso exista
    if telefone.startswith('55'):
        telefone = telefone[2:]
    return telefone

columns_to_hide_from_final_df_leads_appointments_sales = [
                    "Telefones Limpos", 
                    "Telefone(s) do cliente",
                    "Dia da Semana",
                ]

def rename_columns_df_leads_with_purchases(df_leads_with_purchases):
    df_leads_with_purchases.columns = [
                                    # Lead columns
                                    "ID lead", "Email do lead", "Telefone do lead", 
                                    "Mensagem", "Unidade do lead", "Fonte", "Dia da entrada", 
                                    "Source", "Medium", "Term", "Content", "Campaign", 
                                    "Mês do lead", "Categoria", 

                                    # Appointment columns
                                    "Data Na Agenda", "Procedimento", "Status Agenda", "Unidade da Agenda", 
                                    
                                    # Sales columns
                                    "Telefones Limpos", "Telefone(s) do cliente", "ID orçamento",
                                    "Data Venda", "Unidade da Venda", "Valor primeiro orçamento", 
                                    "Total comprado pelo cliente", "Número de orçamentos do cliente",
                                    "Dia", "Mês da Venda", "Dia da Semana", "comprou"
                                ]
    return df_leads_with_purchases