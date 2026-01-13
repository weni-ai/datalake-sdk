import os

from weni_datalake_sdk.clients.redshift.redshift_client import query_dc_api


def get_installed_apps(**kwargs) -> dict:
    metric = os.environ.get("INSTALLED_APPS_METRIC_NAME")

    try:
        result = query_dc_api(metric=metric, query_params=kwargs)
        data = result.json()
        return data

    except Exception as e:
        raise Exception(f"Error querying installed apps: {e}")