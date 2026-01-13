from unittest import mock

import pytest

from weni_datalake_sdk.clients.redshift.installed_apps import get_installed_apps


class TestGetInstalledApps:
    @pytest.fixture
    def mock_env_metric(self, monkeypatch):
        monkeypatch.setenv("INSTALLED_APPS_METRIC_NAME", "test_metric_installed_apps")

    def test_get_installed_apps_success(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.installed_apps.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {"data": "installed_apps"}
            mock_query.return_value = mock_response

            result = get_installed_apps(
                account="acc_123",
                date_start="2023-01-01",
                date_end="2023-01-31",
                extra="param",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_installed_apps",
                query_params={
                    "account": "acc_123",
                    "date_start": "2023-01-01",
                    "date_end": "2023-01-31",
                    "extra": "param",
                },
            )
            assert result == {"data": "installed_apps"}

    def test_get_installed_apps_without_optional_params(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.installed_apps.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {"data": "installed_apps"}
            mock_query.return_value = mock_response

            result = get_installed_apps()

            mock_query.assert_called_once_with(
                metric="test_metric_installed_apps",
                query_params={},
            )
            assert result == {"data": "installed_apps"}

    def test_get_installed_apps_api_error(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.installed_apps.query_dc_api"
        ) as mock_query:
            mock_query.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                get_installed_apps(
                    account="acc_123", date_start="2023-01-01", date_end="2023-01-31"
                )
            assert "Error querying installed apps: API Error" in str(exc_info.value)

