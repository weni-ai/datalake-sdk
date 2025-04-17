from weni_datalake_sdk.clients.redshift.redshift_client import query_redshift


def get_traces(query_params: dict = None) -> dict:
    try:
        result = query_redshift(metric="weni-traces", query_params=query_params)
        return result

    except Exception as e:
        raise Exception(f"Error querying traces: {e}")
