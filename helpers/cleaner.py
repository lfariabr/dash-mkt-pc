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