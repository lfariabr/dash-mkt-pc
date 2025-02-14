"""
Module for collecting data from Google Sheets
"""
from typing import Dict, List
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

class GoogleSheetsCollector:
    def __init__(self, credentials_path: str):
        """Initialize Google Sheets collector with credentials"""
        self.creds = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        self.service = build('sheets', 'v4', credentials=self.creds)
    
    def get_leads_data(self, spreadsheet_id: str, range_name: str) -> pd.DataFrame:
        """
        Fetch leads data from the Leads x Appointment spreadsheet
        Returns DataFrame with leads information
        """
        sheet = self.service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()
            
        return pd.DataFrame(values[1:], columns=values[0])
    
    def get_sales_data(self, spreadsheet_id: str, range_name: str) -> pd.DataFrame:
        """
        Fetch sales data from the Sales spreadsheet
        Returns DataFrame with sales information
        """
        # Similar implementation to get_leads_data
        pass
