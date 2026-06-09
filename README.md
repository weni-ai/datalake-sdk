# Weni Data Lake SDK

The Weni Data Lake SDK is a Python library that provides an interface to interact with Weni's data lake services. It supports operations for sending data (messages, traces, events, message templates, and commerce webhooks) and querying data from Redshift via the Data Consumption API.

## Installation

```bash
pip install weni-datalake-sdk
```

If you are using Poetry, add the package to your project with:

```bash
poetry add weni-datalake-sdk
```

## Environment Variables

### Inserting data

```bash
DATALAKE_SERVER_ADDRESS=your_server_address
DATALAKE_MAXIMUN_WORKERS=5  # optional, default is 5 — used by async send methods
```

### Querying data

```bash
REDSHIFT_QUERY_BASE_URL=your_redshift_url
REDSHIFT_SECRET=your_secret
REDSHIFT_ROLE_ARN=your_role_arn
```

### Metric names (one per query method)

```bash
# Message templates and traces
MESSAGE_TEMPLATES_METRIC_NAME=your_metric_name
TRACES_METRIC_NAME=your_trace_metric_name

# Installed apps
INSTALLED_APPS_METRIC_NAME=your_installed_apps_metric_name

# Events (bronze)
EVENTS_METRIC_NAME=your_event_metric_name
EVENTS_COUNT_METRIC_NAME=your_events_count_metric_name
EVENTS_SUM_METRIC_NAME=your_events_sum_metric_name
EVENTS_AVG_METRIC_NAME=your_events_avg_metric_name
EVENTS_MAX_METRIC_NAME=your_events_max_metric_name
EVENTS_MIN_METRIC_NAME=your_events_min_metric_name
EVENTS_COUNT_BY_GROUP_METRIC_NAME=your_events_count_by_group_metric_name
EVENTS_UNIQUE_CONTACT_URNS_METRIC_NAME=your_events_unique_contact_urns_metric_name
EVENTS_RECURRING_CONTACT_URNS_METRIC_NAME=your_events_recurring_contact_urns_metric_name

# Events (silver)
EVENTS_SILVER_METRIC_NAME=your_events_silver_metric_name
EVENTS_SILVER_COUNT_METRIC_NAME=your_events_silver_count_metric_name
EVENTS_SILVER_COUNT_BY_GROUP_METRIC_NAME=your_events_silver_count_by_group_metric_name
EVENTS_SILVER_UNIQUE_CONTACT_URNS_METRIC_NAME=your_events_silver_unique_contact_urns_metric_name
EVENTS_SILVER_RECURRING_CONTACT_URNS_METRIC_NAME=your_events_silver_recurring_contact_urns_metric_name
```

### AWS credentials

AWS credentials are required to assume the role and fetch secrets from AWS Secrets Manager:

```bash
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_DEFAULT_REGION=your_region
```

The SDK uses an assumed role (`REDSHIFT_ROLE_ARN`) to authenticate with the Data Consumption API.

## Usage Examples

### 1. Sending data

```python
from weni_datalake_sdk.clients.client import send_data
from weni_datalake_sdk.paths.msg_path import MsgPath

data = {
    "project_uuid": "68c84e84-2d7d-4dc7-8193-50d0e2321b2e",
    "text": "Hello!",
}

send_data(MsgPath, data)
```

You can pass a path class or an instantiated path object.

### 2. Send trace data

```python
from weni_datalake_sdk.clients.client import send_trace_data
from weni_datalake_sdk.paths.trace_path import TracePath

data = {
    "project_uuid": "68c84e84-2d7d-4dc7-8193-50d0e2321b2e",
    "receive": "trace content",
}

send_trace_data(TracePath, data)
```

### 3. Send message template data

```python
from weni_datalake_sdk.clients.client import (
    send_message_template_data,
    send_message_template_data_async,
)
from weni_datalake_sdk.paths.message_template_path import MessageTemplatePath

data = {
    "template_id": "template123",
    "content": "Hello!",
}

# Synchronous
send_message_template_data(MessageTemplatePath, data)

# Asynchronous — returns a Future
future = send_message_template_data_async(MessageTemplatePath, data)
future.result()
```

### 4. Send message template status data

```python
from weni_datalake_sdk.clients.client import (
    send_message_template_status_data,
    send_message_template_status_data_async,
)
from weni_datalake_sdk.paths.message_template_status_path import MessageTemplateStatusPath

data = {
    "template_id": "template123",
    "status": "approved",
}

# Synchronous
send_message_template_status_data(MessageTemplateStatusPath, data)

# Asynchronous — returns a Future
future = send_message_template_status_data_async(MessageTemplateStatusPath, data)
future.result()
```

### 5. Send event data

```python
from weni_datalake_sdk.clients.client import send_event_data, send_event_data_async
from weni_datalake_sdk.paths.events_path import EventPath

data = {
    "event_name": "event_name",
    "key": "key",
    "value": "value",
    "value_type": "string",  # string, int, list, or bool
    "date": "2024-01-01T00:00:00Z",
    "project": "project_uuid",
    "contact_urn": "contact_urn",
    "metadata": {
        "field1": "value1",
        "field2": "value2",
    },
}

# Synchronous
send_event_data(EventPath, data)

# Asynchronous — returns a Future
future = send_event_data_async(EventPath, data)
future.result()
```

### 6. Send commerce webhook data

```python
from weni_datalake_sdk.clients.client import send_commerce_webhook_data
from weni_datalake_sdk.paths.commerce_webhook import CommerceWebhookPath
from datetime import datetime

data = {
    "status": 1,
    "template": "template_name",
    "template_variables": {"foo": "bar"},
    "contact_urn": "whatsapp:+55123456789",
    "error": {"msg": "error"},
    "data": {"foo": "bar"},
    "date": datetime.now().isoformat(),
    "project": "your-project-uuid",
    "request": {"req": "value"},
    "response": {"res": "value"},
    "agent": "some-uuid",
}

send_commerce_webhook_data(CommerceWebhookPath, data)
```

All fields are optional. For Struct fields, use dicts. For date, use an ISO string. Omit fields you do not want to send, or set them to `None`.

### 7. Get message templates

```python
from weni_datalake_sdk.clients.redshift.message_templates import get_message_templates

result = get_message_templates(
    contact_urn="contact123",
    template_uuid="template_uuid",
)
```

You can also pass a `query_params` dict for additional filters.

### 8. Get traces

```python
from weni_datalake_sdk.clients.redshift.traces import get_traces

result = get_traces(
    query_params={
        "message_uuid": "123e4567-e89b-12d3-a456-426614174000",
    }
)
```

### 9. Get installed apps

```python
from weni_datalake_sdk.clients.redshift.installed_apps import get_installed_apps

result = get_installed_apps(
    account="acc_123",
    date_start="2023-01-01",
    date_end="2023-01-31",
)
```

All parameters are optional.

### 10. Get events

```python
from weni_datalake_sdk.clients.redshift.events import get_events

result = get_events(
    project="project_uuid",       # required
    date_start="2025-06-03T00:00:00Z",  # required
    date_end="2025-07-30T23:59:59Z",    # required
    event_type="event_type",      # optional
    event_name="event_name",      # optional
    key="key",                    # optional
    value="value",                # optional
    value_type="value_type",      # optional
    contact_urn="contact_urn",    # optional
)
```

### 11. Get events count

```python
from weni_datalake_sdk.clients.redshift.events import get_events_count

result = get_events_count(
    project="your_project_uuid",          # required
    date_start="2025-06-03T00:00:00Z",    # required
    date_end="2025-07-30T23:59:59Z",      # required
    event_type="event_type",              # optional
    event_name="event_name",              # optional
    key="topics",                         # optional
    value="value",                        # optional
    value_type="value_type",              # optional
    contact_urn="contact_urn",            # optional
    agent_uuid="abc-123",                 # optional
)
```

### 12. Get events sum, avg, max, and min

```python
from weni_datalake_sdk.clients.redshift.events import (
    get_events_sum,
    get_events_avg,
    get_events_max,
    get_events_min,
)

# All four methods share the same required and optional parameters
result_sum = get_events_sum(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    key="score",
    event_name="weni_nps",
)

result_avg = get_events_avg(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    key="score",
    event_name="weni_nps",
)

result_max = get_events_max(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    key="score",
    event_name="weni_nps",
)

result_min = get_events_min(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    key="score",
    event_name="weni_nps",
)
```

### 13. Get events count by group

```python
from weni_datalake_sdk.clients.redshift.events import get_events_count_by_group

result = get_events_count_by_group(
    project="your_project_uuid",          # required
    date_start="2025-06-03T00:00:00Z",    # required
    date_end="2025-07-30T23:59:59Z",      # required
    metadata_key="topic_uuid",            # required
    event_type="event_type",              # optional
    event_name="event_name",              # optional
    key="topics",                         # optional
    value="value",                        # optional
    value_type="value_type",              # optional
    contact_urn="contact_urn",            # optional
    group_by="subtopic_uuid",             # optional
    metadata_value="uuid",                # optional
)
```

If you do not pass `group_by`, the result is aggregated by `value`.

### 14. Get unique and recurring contact URNs

```python
from weni_datalake_sdk.clients.redshift.events import (
    get_events_unique_contact_urns,
    get_events_recurring_contact_urns,
)

# Unique contact URNs — all parameters are optional
unique_urns = get_events_unique_contact_urns(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    event_name="event_name",
    key="key",
)

# Recurring contact URNs — project, date_start, and date_end are required
recurring_urns = get_events_recurring_contact_urns(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    event_name="event_name",
    key="key",
)
```

### 15. Get events from silver tables

```python
from weni_datalake_sdk.clients.redshift.events import get_events_silver

result = get_events_silver(
    project="your_project_uuid",          # required
    date_start="2025-06-03T00:00:00Z",    # required
    date_end="2025-07-30T23:59:59Z",      # required
    table="topics",                       # required
    event_name="event_name",              # optional
    key="key",                            # optional
    contact_urn="contact_urn",            # optional
)
```

### 16. Get events count from silver tables

```python
from weni_datalake_sdk.clients.redshift.events import get_events_silver_count

result = get_events_silver_count(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    table="topics",
)
```

### 17. Get events count from silver tables by group

```python
from weni_datalake_sdk.clients.redshift.events import get_events_silver_count_by_group

result = get_events_silver_count_by_group(
    project="your_project_uuid",          # required
    date_start="2025-06-03T00:00:00Z",    # required
    date_end="2025-07-30T23:59:59Z",      # required
    table="topics",                       # required
    metadata_key="topic_uuid",            # required
    group_by="subtopic_uuid",             # optional
    metadata_value="uuid",                # optional
)
```

### 18. Get unique and recurring contact URNs from silver tables

```python
from weni_datalake_sdk.clients.redshift.events import (
    get_events_silver_unique_contact_urns,
    get_events_silver_recurring_contact_urns,
)

unique_urns = get_events_silver_unique_contact_urns(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    table="conversation_classification",
    event_name="weni_nexus_data",
    key="conversation_classification",
)

recurring_urns = get_events_silver_recurring_contact_urns(
    project="your_project_uuid",
    date_start="2025-06-03T00:00:00Z",
    date_end="2025-07-30T23:59:59Z",
    table="conversation_classification",
    event_name="weni_nexus_data",
    key="conversation_classification",
)
```

#### Valid silver tables

`topics`, `weni_csat`, `weni_nps`, `conversation_classification`, `conversion_lead`

Silver query methods accept the same optional filters as the bronze `get_events` methods (`event_name`, `key`, `value`, `value_type`, `contact_urn`, `metadata_key`, `metadata_value`, `agent_uuid`, etc.).

## Error Handling

The SDK includes proper error handling. Always wrap your calls in try-except blocks:

```python
try:
    result = get_message_templates(template_uuid="template123")
except Exception as e:
    print(f"Error: {e}")
```

## Best Practices

1. **Environment variables**: Ensure all required environment variables are set before using the SDK.
2. **Path validation**: Use proper path classes instead of raw strings.
3. **Error handling**: Implement proper error handling in your code.
4. **Data types**: Ensure you are passing the correct data types for each parameter.
5. **Security**: Never hardcode sensitive information like tokens or credentials.

## Common Issues and Solutions

1. **Connection issues**
   - Ensure `DATALAKE_SERVER_ADDRESS` is correct and accessible
   - Check your network connectivity

2. **Authentication errors**
   - Verify your AWS credentials are properly configured
   - Check if `REDSHIFT_SECRET` and `REDSHIFT_ROLE_ARN` are correct

3. **Missing environment variables**
   - Double-check all required environment variables are set
   - Use a `.env` file for local development

## Contributing

For contributing to this SDK, follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
