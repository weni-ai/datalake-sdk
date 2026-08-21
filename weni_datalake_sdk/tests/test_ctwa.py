from unittest import mock

import pytest

from weni_datalake_sdk.clients.redshift.ctwa import (
    get_ctwa,
    get_ctwa_by_campaign,
)


class TestGetCtwa:
    @pytest.fixture
    def mock_env_metric(self, monkeypatch):
        monkeypatch.setenv("CTWA_METRIC_NAME", "test_metric_ctwa")

    def test_get_ctwa_success(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {
                "values": [
                    {"event_name": "ctwa_event", "value": "conversation_started"}
                ]
            }
            mock_query.return_value = mock_response

            result = get_ctwa(
                project="project-uuid",
                date_start="2026-01-01",
                date_end="2026-01-31",
                campaign_source="campaign-123",
                limit="100",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_ctwa",
                query_params={
                    "project": "project-uuid",
                    "date_start": "2026-01-01",
                    "date_end": "2026-01-31",
                    "campaign_source": "campaign-123",
                    "limit": "100",
                },
            )
            assert result["values"][0]["event_name"] == "ctwa_event"

    def test_get_ctwa_missing_project(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_ctwa(date_start="2026-01-01", date_end="2026-01-31")

        assert "Project is required" in str(exc_info.value)

    def test_get_ctwa_api_error(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_query.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                get_ctwa(project="project-uuid")

            assert "Error querying ctwa: API Error" in str(exc_info.value)


class TestGetCtwaByCampaign:
    @pytest.fixture
    def mock_env_metric(self, monkeypatch):
        monkeypatch.setenv(
            "CTWA_BY_CAMPAIGN_METRIC_NAME",
            "test_metric_ctwa_by_campaign",
        )

    def test_get_ctwa_by_campaign_success(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {
                "values": [
                    {
                        "campaign_source": "campaign-123",
                        "conversation_started": 10,
                        "lead_qualified": 5,
                        "purchase_completed": 2,
                        "order_value": 1500.0,
                    }
                ]
            }
            mock_query.return_value = mock_response

            result = get_ctwa_by_campaign(
                project="project-uuid",
                date_start="2026-01-01",
                date_end="2026-01-31",
                waba="waba-id",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_ctwa_by_campaign",
                query_params={
                    "project": "project-uuid",
                    "date_start": "2026-01-01",
                    "date_end": "2026-01-31",
                    "waba": "waba-id",
                },
            )
            assert result["values"][0]["campaign_source"] == "campaign-123"

    def test_get_ctwa_by_campaign_missing_project(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_ctwa_by_campaign(date_start="2026-01-01", date_end="2026-01-31")

        assert "Project is required" in str(exc_info.value)

    def test_get_ctwa_by_campaign_missing_date_start(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_ctwa_by_campaign(project="project-uuid", date_end="2026-01-31")

        assert "Date start is required" in str(exc_info.value)

    def test_get_ctwa_by_campaign_missing_date_end(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_ctwa_by_campaign(project="project-uuid", date_start="2026-01-01")

        assert "Date end is required" in str(exc_info.value)

    def test_get_ctwa_by_campaign_strips_time_from_dates(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = [{"campaign_source": "campaign-123"}]
            mock_query.return_value = mock_response

            get_ctwa_by_campaign(
                project="project-uuid",
                date_start="2026-08-19T00:00:00",
                date_end="2026-08-21T23:59:59",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_ctwa_by_campaign",
                query_params={
                    "project": "project-uuid",
                    "date_start": "2026-08-19",
                    "date_end": "2026-08-22",
                },
            )

    def test_get_ctwa_by_campaign_keeps_date_only_bounds(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = [{"campaign_source": "campaign-123"}]
            mock_query.return_value = mock_response

            get_ctwa_by_campaign(
                project="project-uuid",
                date_start="2026-08-19",
                date_end="2026-08-21",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_ctwa_by_campaign",
                query_params={
                    "project": "project-uuid",
                    "date_start": "2026-08-19",
                    "date_end": "2026-08-21",
                },
            )

    def test_get_ctwa_by_campaign_empty_list_returns_zeros(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = [{}]
            mock_query.return_value = mock_response

            result = get_ctwa_by_campaign(
                project="project-uuid",
                date_start="2026-08-19",
                date_end="2026-08-21",
            )

            assert result == [
                {
                    "campaign_source": None,
                    "project": "project-uuid",
                    "waba": None,
                    "channel": None,
                    "conversation_started": 0,
                    "lead_qualified": 0,
                    "purchase_completed": 0,
                    "order_value": 0.0,
                }
            ]

    def test_get_ctwa_by_campaign_api_error(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.ctwa.query_dc_api"
        ) as mock_query:
            mock_query.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                get_ctwa_by_campaign(
                    project="project-uuid",
                    date_start="2026-01-01",
                    date_end="2026-01-31",
                )

            assert "Error querying ctwa by campaign: API Error" in str(exc_info.value)
