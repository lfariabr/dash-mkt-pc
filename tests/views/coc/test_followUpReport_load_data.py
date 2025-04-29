import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest
import pandas as pd
from unittest.mock import patch, AsyncMock
from views.st_coc import followUpReport_view

def test_load_data_success(monkeypatch):
    # Patch fetch_all_data to return fake data
    fake_entries = [{'foo': 1}]
    fake_comments = [{'bar': 2}]
    fake_sales = [{'baz': 3}]
    monkeypatch.setattr(followUpReport_view, 'fetch_all_data', AsyncMock(return_value=(fake_entries, fake_comments, fake_sales)))
    # Patch asyncio.run to just call the coroutine
    import asyncio
    monkeypatch.setattr(asyncio, 'run', lambda coro: asyncio.get_event_loop().run_until_complete(coro))
    df_entries, df_comments, df_sales = followUpReport_view.load_data('2024-01-01', '2024-01-31')
    assert not df_entries.empty and 'foo' in df_entries.columns
    assert not df_comments.empty and 'bar' in df_comments.columns
    assert not df_sales.empty and 'baz' in df_sales.columns

def test_load_data_no_dates():
    df_entries, df_comments, df_sales = followUpReport_view.load_data(None, None)
    assert df_entries.empty and df_comments.empty and df_sales.empty
