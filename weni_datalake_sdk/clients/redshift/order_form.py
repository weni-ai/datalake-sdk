import os
from typing import Optional

from weni_datalake_sdk.clients.redshift.redshift_client import query_dc_api


def get_order_form_abandoned_carts(
    account_name: str,
    dt_start: str,
    dt_end: str,
    sales_channel: Optional[str] = None,
) -> dict:
    metric = os.environ.get("ORDER_FORM_ABANDONED_CARTS_METRIC_NAME")

    if not account_name:
        raise Exception("Account name is required")

    if not dt_start:
        raise Exception("Date start is required")

    if not dt_end:
        raise Exception("Date end is required")

    query_params = {
        "account_name": account_name,
        "dt_start": dt_start,
        "dt_end": dt_end,
    }

    if sales_channel:
        query_params["sales_channel"] = sales_channel

    try:
        result = query_dc_api(metric=metric, query_params=query_params)
        data = result.json()
        return data

    except Exception as e:
        raise Exception(f"Error querying order form abandoned carts: {e}")
