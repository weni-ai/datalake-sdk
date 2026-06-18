import os
from typing import Optional

from weni_datalake_sdk.clients.redshift.redshift_client import query_dc_api


def get_orders_shopping_assistant(
    date_start: str,
    date_end: str,
    hostname: Optional[str] = None,
    status: Optional[str] = None,
    value: Optional[str] = None,
) -> dict:
    metric = os.environ.get("ORDERS_SHOPPING_ASSISTANT_METRIC_NAME")

    if not date_start:
        raise Exception("Date start is required")

    if not date_end:
        raise Exception("Date end is required")

    query_params = {
        "date_start": date_start,
        "date_end": date_end,
    }

    if hostname:
        query_params["hostname"] = hostname
    if status:
        query_params["status"] = status
    if value:
        query_params["value"] = value

    try:
        result = query_dc_api(metric=metric, query_params=query_params)
        data = result.json()
        return data

    except Exception as e:
        raise Exception(f"Error querying shopping assistant orders: {e}")
