from datetime import datetime
from weni.contracts.msg import Msg

def test_msg_contract():
    msg = Msg(
        project_uuid="123e4567-e89b-12d3-a456-426614174000",
        text="Oi!",
        extra_fields=dict(id="123",
        sender="user1",
        receiver="user2",
        timestamp=datetime.utcnow(),
        metadata={"source": "app"})
    )
    assert msg.extra_fields.get("id") == "123"
    assert msg.text == "Oi!"
