import os

from weni_datalake_sdk.clients.redshift.events import clean_quotes
from weni_datalake_sdk.clients.redshift.redshift_client import query_dc_api


def get_ctwa(**kwargs) -> dict:
    metric = os.environ.get("CTWA_METRIC_NAME")

    if not kwargs.get("project"):
        raise Exception("Project is required")

    try:
        result = query_dc_api(metric=metric, query_params=kwargs)
        data = result.json()
        return clean_quotes(data)

    except Exception as e:
        raise Exception(f"Error querying ctwa: {e}")


def get_ctwa_by_campaign(**kwargs) -> dict:
    metric = os.environ.get("CTWA_BY_CAMPAIGN_METRIC_NAME")

    if not kwargs.get("project"):
        raise Exception("Project is required")

    try:
        result = query_dc_api(metric=metric, query_params=kwargs)
        data = result.json()
        return data

    except Exception as e:
        raise Exception(f"Error querying ctwa by campaign: {e}")
