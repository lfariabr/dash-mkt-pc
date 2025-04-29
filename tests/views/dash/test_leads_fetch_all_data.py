import sys
from pathlib import Path
# Add project root to sys.path for robust import resolution
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest
from unittest.mock import patch
import pandas as pd
from views.st_dash.lead_view import load_data

def test_load_data_returns_dataframe_from_api():
    mock_data = [
        {'ID do lead': 1, 'Dia da entrada': '2024-01-01', 'Unidade': 'A', 'Status': 'Não Conseguiu Contato'},
        {'ID do lead': 2, 'Dia da entrada': '2024-01-02', 'Unidade': 'B', 'Status': 'Não Atendido'}
    ]
    with patch('views.st_dash.lead_view.fetch_and_process_lead_report', return_value=mock_data):
        df = load_data('2024-01-01', '2024-01-31')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert set(df.columns) >= {'ID do lead', 'Dia da entrada', 'Unidade', 'Status'}
        assert df.iloc[0]['ID do lead'] == 1
        assert df.iloc[1]['Status'] == 'Não Atendido'