from unittest import mock

import pytest

from weni_datalake_sdk.clients.redshift.order_form import (
    get_order_form_abandoned_carts,
)


class TestGetOrderFormAbandonedCarts:
    @pytest.fixture
    def mock_env_metric(self, monkeypatch):
        monkeypatch.setenv(
            "ORDER_FORM_ABANDONED_CARTS_METRIC_NAME",
            "test_metric_order_form_abandoned_carts",
        )

    def test_get_order_form_abandoned_carts_success(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.order_form.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = [
                {
                    "profile_id": "profile-123",
                    "order_form_id": "of-456",
                    "last_total_value": 150.0,
                }
            ]
            mock_query.return_value = mock_response

            result = get_order_form_abandoned_carts(
                account_name="superangeloni",
                dt_start="2026-04-01 00:00:00",
                dt_end="2026-07-13 00:00:00",
                sales_channel="1",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_order_form_abandoned_carts",
                query_params={
                    "account_name": "superangeloni",
                    "dt_start": "2026-04-01 00:00:00",
                    "dt_end": "2026-07-13 00:00:00",
                    "sales_channel": "1",
                },
            )
            assert result[0]["profile_id"] == "profile-123"

    def test_get_order_form_abandoned_carts_without_sales_channel(
        self, mock_env_metric
    ):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.order_form.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = []
            mock_query.return_value = mock_response

            result = get_order_form_abandoned_carts(
                account_name="superangeloni",
                dt_start="2026-04-01 00:00:00",
                dt_end="2026-07-13 00:00:00",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_order_form_abandoned_carts",
                query_params={
                    "account_name": "superangeloni",
                    "dt_start": "2026-04-01 00:00:00",
                    "dt_end": "2026-07-13 00:00:00",
                },
            )
            assert result == []

    def test_get_order_form_abandoned_carts_missing_account_name(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_order_form_abandoned_carts(
                account_name="",
                dt_start="2026-04-01 00:00:00",
                dt_end="2026-07-13 00:00:00",
            )

        assert "Account name is required" in str(exc_info.value)

    def test_get_order_form_abandoned_carts_missing_dt_start(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_order_form_abandoned_carts(
                account_name="superangeloni",
                dt_start="",
                dt_end="2026-07-13 00:00:00",
            )

        assert "Date start is required" in str(exc_info.value)

    def test_get_order_form_abandoned_carts_missing_dt_end(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_order_form_abandoned_carts(
                account_name="superangeloni",
                dt_start="2026-04-01 00:00:00",
                dt_end="",
            )

        assert "Date end is required" in str(exc_info.value)

    def test_get_order_form_abandoned_carts_api_error(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.order_form.query_dc_api"
        ) as mock_query:
            mock_query.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                get_order_form_abandoned_carts(
                    account_name="superangeloni",
                    dt_start="2026-04-01 00:00:00",
                    dt_end="2026-07-13 00:00:00",
                )

            assert "Error querying order form abandoned carts: API Error" in str(
                exc_info.value
            )
