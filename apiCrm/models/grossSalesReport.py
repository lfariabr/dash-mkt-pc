# https://crm.eprocorpo.com.br/reports/gross-sales

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class GrossSalesReport(Base):
    __tablename__ = "gross_sales_report"
    id = Column(Integer, primary_key=True, index=True)
    
"""
query = '''
query GrossSalesReport($start: Date!, $end: Date!) {
  grossSalesReport(
    filters: { createdAtRange: { start: $start, end: $end } }
    pagination: { currentPage: 1, perPage: 100 }
  ) {
    data {
      bill {
        id
        quote {
          createdBy {
            name
            createdAt
          }
          comments
          customer {
            name
            id
            telephones {
              number
            }
          }
          bill {
            items {
              procedure {
                name
              }
              quantity
            }
            quote {
              evaluations {
                employee {
                  name
                }
              }
            }
          }
        }
      }
      statusLabel
      isReseller
      store {
        name
      }
      customerSignedAt
    }
  }
}
'''
"""