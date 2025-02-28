# import pytest
# from datetime import datetime
# from weni.contracts.validator import validate_contract
# from weni.paths.msg_path import MsgPath
# from weni.contracts.msg import Msg

# def test_valid_contract():
#     msg = Msg(id="123", text="Oi!", sender="user1", org_id="456")
#     assert validate_contract(MsgPath, msg) is True

# def test_missing_optional_fields():
#     msg = Msg(id="123", org_id="456")  # Apenas 'id', sem problema
#     assert validate_contract(MsgPath, msg) is True

# def test_extra_fields():
#     msg_data = {
#         "id": "123",
#         "org_id": "456",
#         "text": "Oi!",
#         "sender": "user1",
#         "extra_field": "Isso não deveria estar aqui"  # Inválido
#     }

#     with pytest.raises(ValueError, match="Campos não permitidos encontrados"):
#         validate_contract(MsgPath, msg_data)

# def test_missing_required_field():
#     msg_data = {"text": "Oi!", "org_id":"15988"}  # Falta o campo 'id'

#     with pytest.raises(TypeError):  # O dataclass exige 'id'
#         Msg(**msg_data)
