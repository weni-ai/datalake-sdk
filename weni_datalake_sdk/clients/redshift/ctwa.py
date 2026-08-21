import os
import re
from datetime import date, datetime, timedelta

from weni_datalake_sdk.clients.redshift.events import clean_quotes
from weni_datalake_sdk.clients.redshift.redshift_client import query_dc_api

# The by-campaign metric returns wrong (often empty) results when the date
# filters carry a time component, so both bounds are sent as plain YYYY-MM-DD.
_DATE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?")


def _to_metric_date(value, is_end: bool) -> str:
    if isinstance(value, datetime):
        day, has_time = value.date(), (value.hour, value.minute, value.second) != (
            0,
            0,
            0,
        )
    elif isinstance(value, date):
        day, has_time = value, False
    else:
        match = _DATE_PATTERN.match(str(value).strip())
        if not match:
            return str(value)
        day = date.fromisoformat(match.group(1))
        has_time = any(part and int(part) for part in match.groups()[1:])

    # A date-only upper bound means midnight, which would drop that whole day.
    if is_end and has_time:
        day += timedelta(days=1)

    return day.isoformat()


def _empty_campaign_metrics(project: str) -> dict:
    return {
        "campaign_source": None,
        "project": project,
        "waba": None,
        "channel": None,
        "conversation_started": 0,
        "lead_qualified": 0,
        "purchase_completed": 0,
        "order_value": 0.0,
    }


def _normalize_campaign_metrics(data, project: str):
    empty_row = _empty_campaign_metrics(project)

    if isinstance(data, list):
        rows = [row for row in data if row]
        return rows or [empty_row]

    if isinstance(data, dict) and "values" in data:
        rows = [row for row in (data.get("values") or []) if row]
        data["values"] = rows or [empty_row]
        return data

    return data


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

    if not kwargs.get("date_start"):
        raise Exception("Date start is required")

    if not kwargs.get("date_end"):
        raise Exception("Date end is required")

    query_params = dict(kwargs)
    query_params["date_start"] = _to_metric_date(kwargs["date_start"], is_end=False)
    query_params["date_end"] = _to_metric_date(kwargs["date_end"], is_end=True)

    try:
        result = query_dc_api(metric=metric, query_params=query_params)
        data = clean_quotes(result.json())
        return _normalize_campaign_metrics(data, kwargs["project"])

    except Exception as e:
        raise Exception(f"Error querying ctwa by campaign: {e}")
