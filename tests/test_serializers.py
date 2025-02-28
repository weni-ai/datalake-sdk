from weni.utils.serializers import serialize_msg
from weni.contracts.msg import Msg
from datetime import datetime

def test_serialize_msg():
    msg = Msg(
        id="123",
        org_id="456",
        text="Oi!",
        sender="user1",
        receiver="user2",
        timestamp=datetime(2024, 2, 1, 12, 0, 0),
        metadata={"key": "value"}
    )
    serialized = serialize_msg(msg)
    
    assert serialized["id"] == "123"
    assert serialized["org_id"] == "456"
    assert serialized["text"] == "Oi!"
    assert serialized["timestamp"] == "2024-02-01T12:00:00"
    assert serialized["metadata"] == '{"key": "value"}'
