from datetime import datetime
from unittest import mock

import pytest

from weni_datalake_sdk.clients.client import send_event_data
from weni_datalake_sdk.clients.redshift.events import get_events
from weni_datalake_sdk.paths.events_path import EventPath


@pytest.fixture
def mock_grpc_stub_events():
    with mock.patch(
        "weni_datalake_sdk.clients.events_pb2_grpc.DatalakeManagerServiceStub"
    ) as mock_stub:
        yield mock_stub


@pytest.fixture
def mock_grpc_channel():
    with mock.patch("grpc.insecure_channel") as mock_channel:
        yield mock_channel


@pytest.fixture
def mock_env_metric(monkeypatch):
    monkeypatch.setenv("EVENTS_METRIC_NAME", "test_metric")


def test_send_event_data_success(mock_grpc_stub_events, mock_grpc_channel):
    mock_stub_instance = mock_grpc_stub_events.return_value
    mock_stub_instance.InsertEventData.return_value.status = "success"

    event_data = {
        "event_name": "test_event",
        "key": "test_key",
        "date": datetime.now().isoformat(),
        "project": "test_project",
        "contact_urn": "test_contact",
        "value_type": "string",
        "value": "test_value",
        "metadata": {"meta_key": "meta_value"},
    }

    status = send_event_data(EventPath, event_data)

    mock_stub_instance.InsertEventData.assert_called_once()
    assert status == "success"


def test_send_event_data_no_date(mock_grpc_stub_events, mock_grpc_channel):
    mock_stub_instance = mock_grpc_stub_events.return_value
    mock_stub_instance.InsertEventData.return_value.status = "success"

    event_data = {
        "event_name": "test_event",
        "key": "test_key",
        "project": "test_project",
        "contact_urn": "test_contact",
        "value_type": "string",
        "value": "test_value",
    }

    status = send_event_data(EventPath, event_data)

    mock_stub_instance.InsertEventData.assert_called_once()
    assert status == "success"


def test_send_event_data_invalid_date(mock_grpc_stub_events, mock_grpc_channel):
    mock_stub_instance = mock_grpc_stub_events.return_value
    mock_stub_instance.InsertEventData.return_value.status = "success"

    event_data = {
        "event_name": "test_event",
        "key": "test_key",
        "date": "invalid-date",
        "project": "test_project",
        "contact_urn": "test_contact",
        "value_type": "string",
        "value": "test_value",
    }

    status = send_event_data(EventPath, event_data)

    mock_stub_instance.InsertEventData.assert_called_once()
    assert status == "success"
    # We can also check if GetCurrentTime was called on the timestamp
    args, kwargs = mock_stub_instance.InsertEventData.call_args
    request = args[0]
    # This is a bit tricky as the timestamp is generated inside the function
    # but we know it should not be the zero value if GetCurrentTime is called
    assert request.date.seconds != 0 or request.date.nanos != 0


def test_send_event_data_no_metadata(mock_grpc_stub_events, mock_grpc_channel):
    mock_stub_instance = mock_grpc_stub_events.return_value
    mock_stub_instance.InsertEventData.return_value.status = "success"

    event_data = {
        "event_name": "test_event",
        "key": "test_key",
        "date": datetime.now().isoformat(),
        "project": "test_project",
        "contact_urn": "test_contact",
        "value_type": "string",
        "value": "test_value",
    }

    status = send_event_data(EventPath, event_data)

    mock_stub_instance.InsertEventData.assert_called_once()
    assert status == "success"
    args, kwargs = mock_stub_instance.InsertEventData.call_args
    request = args[0]
    assert not request.metadata.fields


def test_get_events_success(mock_env_metric):
    with mock.patch(
        "weni_datalake_sdk.clients.redshift.events.query_dc_api"
    ) as mock_query:
        mock_response = mock.Mock()
        mock_response.json.return_value = {"data": "test"}
        mock_query.return_value = mock_response

        result = get_events(
            project="test_project",
            date_start="2023-01-01",
            date_end="2023-01-31",
            extra="param",
        )

        mock_query.assert_called_once_with(
            metric="test_metric",
            query_params={
                "project": "test_project",
                "date_start": "2023-01-01",
                "date_end": "2023-01-31",
                "extra": "param",
            },
        )
        assert result == {"data": "test"}


def test_get_events_missing_project():
    with pytest.raises(Exception) as exc_info:
        get_events(date_start="2023-01-01", date_end="2023-01-31")
    assert str(exc_info.value) == "Project is required"


def test_get_events_missing_date_start():
    with pytest.raises(Exception) as exc_info:
        get_events(project="test_project", date_end="2023-01-31")
    assert str(exc_info.value) == "Date start is required"


def test_get_events_missing_date_end():
    with pytest.raises(Exception) as exc_info:
        get_events(project="test_project", date_start="2023-01-01")
    assert str(exc_info.value) == "Date end is required"


def test_get_events_api_error(mock_env_metric):
    with mock.patch(
        "weni_datalake_sdk.clients.redshift.events.query_dc_api"
    ) as mock_query:
        mock_query.side_effect = Exception("API Error")

        with pytest.raises(Exception) as exc_info:
            get_events(
                project="test_project",
                date_start="2023-01-01",
                date_end="2023-01-31",
            )
        assert "Error querying events: API Error" in str(exc_info.value)


def test_get_events_count_by_group_success(monkeypatch):
    monkeypatch.setenv("EVENTS_COUNT_BY_GROUP_METRIC_NAME", "test_metric_group")
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with mock.patch(
        "weni_datalake_sdk.clients.redshift.events.query_dc_api"
    ) as mock_query:
        mock_response = mock.Mock()
        mock_response.json.return_value = {"data": "grouped"}
        mock_query.return_value = mock_response

        result = get_events_count_by_group(
            project="test_project",
            date_start="2023-01-01",
            date_end="2023-01-31",
            metadata_key="topic_uuid",
            extra="param",
        )

        mock_query.assert_called_once_with(
            metric="test_metric_group",
            query_params={
                "project": "test_project",
                "date_start": "2023-01-01",
                "date_end": "2023-01-31",
                "metadata_key": "topic_uuid",
                "extra": "param",
            },
        )
        assert result == {"data": "grouped"}


def test_get_events_count_by_group_missing_project():
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with pytest.raises(Exception) as exc_info:
        get_events_count_by_group(
            date_start="2023-01-01", date_end="2023-01-31", metadata_key="topic_uuid"
        )
    assert str(exc_info.value) == "Project is required"


def test_get_events_count_by_group_missing_date_start():
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with pytest.raises(Exception) as exc_info:
        get_events_count_by_group(
            project="test_project", date_end="2023-01-31", metadata_key="topic_uuid"
        )
    assert str(exc_info.value) == "Date start is required"


def test_get_events_count_by_group_missing_date_end():
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with pytest.raises(Exception) as exc_info:
        get_events_count_by_group(
            project="test_project", date_start="2023-01-01", metadata_key="topic_uuid"
        )
    assert str(exc_info.value) == "Date end is required"


def test_get_events_count_by_group_missing_metadata_key():
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with pytest.raises(Exception) as exc_info:
        get_events_count_by_group(
            project="test_project", date_start="2023-01-01", date_end="2023-01-31"
        )
    assert str(exc_info.value) == "metadata_key is required"


def test_get_events_count_by_group_api_error(monkeypatch):
    monkeypatch.setenv("EVENTS_COUNT_BY_GROUP_METRIC_NAME", "test_metric_group")
    from weni_datalake_sdk.clients.redshift.events import (
        get_events_count_by_group,
    )

    with mock.patch(
        "weni_datalake_sdk.clients.redshift.events.query_dc_api"
    ) as mock_query:
        mock_query.side_effect = Exception("API Error")
        with pytest.raises(Exception) as exc_info:
            get_events_count_by_group(
                project="test_project",
                date_start="2023-01-01",
                date_end="2023-01-31",
                metadata_key="topic_uuid",
            )
        assert "Error querying events count: API Error" in str(exc_info.value)
