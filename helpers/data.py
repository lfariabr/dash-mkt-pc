mkt_lead_data = {
                # Lead fields
                "lead_id": str(row["ID do lead"]) if pd.notna(row["ID do lead"]) else None,
                "lead_email": row["Email do lead"] if pd.notna(row["Email do lead"]) else None,
                "lead_phone": row["Telefone do lead"] if pd.notna(row["Telefone do lead"]) else None,
                "lead_message": row["Mensagem"] if pd.notna(row["Mensagem"]) else None,
                "lead_store": row["Unidade do lead"] if pd.notna(row["Unidade do lead"]) else None,
                "lead_source": row["Fonte"] if pd.notna(row["Fonte"]) else None,
                "lead_entry_day": int(row["Dia da entrada"].day) if pd.notna(row["Dia da entrada"]) else None,
                "lead_mkt_source": row["Source"] if pd.notna(row["Source"]) else None,
                "lead_mkt_medium": row["Medium"] if pd.notna(row["Medium"]) else None,
                "lead_mkt_term": row["Term"] if pd.notna(row["Term"]) else None,
                "lead_mkt_content": row["Content"] if pd.notna(row["Content"]) else None,
                "lead_mkt_campaign": row["Campaign"] if pd.notna(row["Campaign"]) else None,
                "lead_month": row["Mês do lead"] if pd.notna(row["Mês do lead"]) else None,
                "lead_category": row["Categoria"] if pd.notna(row["Categoria"]) else None,
                
                # Appointment fields
                "appointment_date": row["data_agenda"] if pd.notna(row["data_agenda"]) else None,
                "appointment_procedure": row["procedimento"] if pd.notna(row["procedimento"]) else None,
                "appointment_status": row["status"] if pd.notna(row["status"]) else None,
                "appointment_store": row["unidade na agenda"] if pd.notna(row["unidade na agenda"]) else None,
                
                # Sales fields
                "sale_cleaned_phone": row["Telefones Limpos"] if pd.notna(row["Telefones Limpos"]) else None,
                "sales_phone": row["Telefone(s) do cliente"] if pd.notna(row["Telefone(s) do cliente"]) else None,
                "sales_quote_id": str(row["ID orçamento"]) if pd.notna(row["ID orçamento"]) else None,
                "sales_date": row["Data venda"] if pd.notna(row["Data venda"]) else None,
                "sales_store": row["Unidade da venda"] if pd.notna(row["Unidade da venda"]) else None,
                "sales_first_quote": str(row["Valor primeiro orçamento"]) if pd.notna(row["Valor primeiro orçamento"]) else None,
                "sales_total_bought": str(row["Total comprado pelo cliente"]) if pd.notna(row["Total comprado pelo cliente"]) else None,
                "sales_number_of_quotes": str(row["Número de orçamentos do cliente"]) if pd.notna(row["Número de orçamentos do cliente"]) else None,
                "sales_day": int(row["Dia"]) if pd.notna(row["Dia"]) else None,
                "sales_month": row["Mês da venda"] if pd.notna(row["Mês da venda"]) else None,
                "sales_day_of_week": row["Dia da Semana"] if pd.notna(row["Dia da Semana"]) else None,
                "sales_purchased": bool(row["comprou"]) if pd.notna(row["comprou"]) else False,
                "sales_interval": int(row["intervalo da compra"]) if pd.notna(row["intervalo da compra"]) else None
}