from unittest import mock

import pytest

from weni_datalake_sdk.clients.redshift.orders import (
    get_orders_shopping_assistant,
)


class TestGetOrdersShoppingAssistant:
    @pytest.fixture
    def mock_env_metric(self, monkeypatch):
        monkeypatch.setenv(
            "ORDERS_SHOPPING_ASSISTANT_METRIC_NAME",
            "test_metric_orders_shopping_assistant",
        )

    def test_get_orders_shopping_assistant_success(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.orders.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {"data": "orders"}
            mock_query.return_value = mock_response

            result = get_orders_shopping_assistant(
                date_start="2026-05-01",
                date_end="2026-06-05",
                hostname="mystore",
                status="payment-approved",
                value="150.00",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_orders_shopping_assistant",
                query_params={
                    "date_start": "2026-05-01",
                    "date_end": "2026-06-05",
                    "hostname": "mystore",
                    "status": "payment-approved",
                    "value": "150.00",
                },
            )
            assert result == {"data": "orders"}

    def test_get_orders_shopping_assistant_without_optional_params(
        self, mock_env_metric
    ):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.orders.query_dc_api"
        ) as mock_query:
            mock_response = mock.Mock()
            mock_response.json.return_value = {"data": "orders"}
            mock_query.return_value = mock_response

            result = get_orders_shopping_assistant(
                date_start="2026-05-01",
                date_end="2026-06-05",
            )

            mock_query.assert_called_once_with(
                metric="test_metric_orders_shopping_assistant",
                query_params={
                    "date_start": "2026-05-01",
                    "date_end": "2026-06-05",
                },
            )
            assert result == {"data": "orders"}

    def test_get_orders_shopping_assistant_missing_date_start(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_orders_shopping_assistant(date_start="", date_end="2026-06-05")

        assert "Date start is required" in str(exc_info.value)

    def test_get_orders_shopping_assistant_missing_date_end(self, mock_env_metric):
        with pytest.raises(Exception) as exc_info:
            get_orders_shopping_assistant(date_start="2026-05-01", date_end="")

        assert "Date end is required" in str(exc_info.value)

    def test_get_orders_shopping_assistant_api_error(self, mock_env_metric):
        with mock.patch(
            "weni_datalake_sdk.clients.redshift.orders.query_dc_api"
        ) as mock_query:
            mock_query.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                get_orders_shopping_assistant(
                    date_start="2026-05-01",
                    date_end="2026-06-05",
                )

            assert "Error querying shopping assistant orders: API Error" in str(
                exc_info.value
            )
