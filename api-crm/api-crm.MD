# API CRM Task

## Models:
1. grossSalesReport 
-> https://crm.eprocorpo.com.br/reports/gross-sales

2. pendingQuotesReport 
-> https://crm.eprocorpo.com.br/reports/pending-quotes

3. leadsReport 
-> https://crm.eprocorpo.com.br/reports/leads

4. appointmentsReport 
-> https://crm.eprocorpo.com.br/reports/appointments

5. salesByPaymentMethodReports 
-> https://crm.eprocorpo.com.br/reports/sales-by-payment-method

query = """
query GrossSalesReport {
  grossSalesReport(
    pagination: { currentPage: 1, perPage: 10 },
    filters: {
      quoteStatus: CONFIRMED
    }
  ) {
    data {
      id
      status
      store {
        id
        name
      }
    }
  }
}
