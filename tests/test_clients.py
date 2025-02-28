import pytest
import requests_mock
from weni.clients.dl_manager_client import DLManagerClient
from weni.contracts.msg import Msg
from weni.paths import MsgPath
from weni.utils.exceptions import ValidationError, DLManagerError


BASE_URL = "https://dl-manager.example.com/api"

@pytest.fixture
def client():
    return DLManagerClient(base_url=BASE_URL)

def test_insert_data_success(client):
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/messages/send", json={"status": "success"}, status_code=200)

        msg_instance = Msg(project_uuid="68c84e84-2d7d-4dc7-8193-50d0e2321b2e", text="Oi!")
        response = client.send(MsgPath, msg_instance)
        assert response == {"status": "success"}

def test_insert_data_fail_contract(client):
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/messages/send", status_code=500, json={"error": "Internal Server Error"})

        with pytest.raises(ValidationError, match="Os dados fornecidos não correspondem ao contrato 'messages'"):
            client.send(MsgPath, {"id": "123", "text": "Oi!"})

def test_insert_data_fail_dl_manager(client):
    with requests_mock.Mocker() as m:
        m.post(f"{BASE_URL}/messages/send", status_code=500, json={"error": "Internal Server Error"})

        with pytest.raises(DLManagerError):
            client.send(MsgPath, Msg(project_uuid="68c84e84-2d7d-4dc7-8193-50d0e2321b2e", text="Oi!"))

# def test_read_data_fail(client):
#     with requests_mock.Mocker() as m:
#         m.get(f"{BASE_URL}/messages/read", status_code=500, json={"error": "Internal Server Error"})  
#         with pytest.raises(DLManagerError):

# def test_read_data_success(client):
#     with requests_mock.Mocker() as m:
#         m.get(f"{BASE_URL}/messages/read", json=[{"id": "123", "text": "Oi!"}], status_code=200)

#         response = client.("messages", {})
#         assert response == [{"id": "123", "text": "Oi!"}]
