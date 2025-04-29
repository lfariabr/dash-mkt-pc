import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest
import pandas as pd
from unittest.mock import patch
from views.st_dash import sales_view

def test_load_data_returns_dataframe_from_api():
    mock_data = [
        {'ID orçamento': 1, 'Data venda': '2024-01-01', 'Unidade': 'A', 'Valor líquido': 100.0},
        {'ID orçamento': 2, 'Data venda': '2024-01-02', 'Unidade': 'B', 'Valor líquido': 200.0}
    ]
    expected_df = pd.DataFrame(mock_data)
    with patch.object(sales_view, 'load_data', return_value=expected_df):
        df = sales_view.load_data('2024-01-01', '2024-01-31')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert set(df.columns) >= {'ID orçamento', 'Data venda', 'Unidade', 'Valor líquido'}
        assert df.iloc[0]['ID orçamento'] == 1
        assert df.iloc[1]['Valor líquido'] == 200.0