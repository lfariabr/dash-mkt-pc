import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from views.st_coc.followUpReport_view import fetch_all_data

@pytest.mark.asyncio
async def test_fetch_all_data_combines_results():
    # Mock the three async API calls
    with patch('views.st_coc.followUpReport_view.fetch_and_process_followUpEntriesReport', new=AsyncMock(return_value=[{'a': 1}])) as mock_entries, \
         patch('views.st_coc.followUpReport_view.fetch_and_process_followUpsCommentsReport', new=AsyncMock(return_value=[{'b': 2}])) as mock_comments, \
         patch('views.st_coc.followUpReport_view.fetch_and_process_grossSales_report', new=AsyncMock(return_value=[{'c': 3}])) as mock_sales:
        entries, comments, sales = await fetch_all_data('2024-01-01', '2024-01-31')
        assert entries == [{'a': 1}]
        assert comments == [{'b': 2}]
        assert sales == [{'c': 3}]
