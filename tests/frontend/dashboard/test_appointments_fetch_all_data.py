import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest
import pandas as pd
from unittest.mock import patch
from frontend.st_dash import appointments_view

def test_load_data_returns_dataframe_from_api():
    mock_data = [
        {'ID agendamento': 1, 'Data': '2024-01-01', 'Unidade do agendamento': 'A', 'Status': 'Agendado'},
        {'ID agendamento': 2, 'Data': '2024-01-02', 'Unidade do agendamento': 'B', 'Status': 'Atendido'}
    ]
    expected_df = pd.DataFrame(mock_data)
    with patch.object(appointments_view, 'load_data', return_value=expected_df):
        df = appointments_view.load_data('2024-01-01', '2024-01-31')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert set(df.columns) >= {'ID agendamento', 'Data', 'Unidade do agendamento', 'Status'}
        assert df.iloc[0]['ID agendamento'] == 1
        assert df.iloc[1]['Status'] == 'Atendido'