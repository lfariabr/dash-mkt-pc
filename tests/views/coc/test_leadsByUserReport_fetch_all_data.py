import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from views.st_coc.leadsByUserReport_view import fetch_leads_and_appointments

@pytest.mark.asyncio
async def test_fetch_leads_and_appointments_combines_results():
    with patch('views.st_coc.leadsByUserReport_view.fetch_and_process_leadsByUserReport', new=AsyncMock(return_value=[{'lead': 1}])) as mock_leads, \
         patch('views.st_coc.leadsByUserReport_view.fetch_and_process_appointment_report_created_at', new=AsyncMock(return_value=[{'appt': 2}])) as mock_appts:
        leads, appts = await fetch_leads_and_appointments('2024-01-01', '2024-01-31')
        assert leads == [{'lead': 1}]
        assert appts == [{'appt': 2}]
