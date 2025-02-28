# import pytest
# from weni.paths import MsgPath
# from weni.contracts import Msg
# from weni.clients.insert import insert_data
# from weni.utils.exceptions import ValidationError

# def test_insert_valid_data():
#     """Testa se um Msg válido pode ser inserido."""
#     msg = Msg(project_uuid="123e4567-e89b-12d3-a456-426614174000", name="Welcome Message", text="Hello!", sender="user1")
    
#     # Não deve lançar erro
#     insert_data(MsgPath, msg)

# def test_insert_missing_required_fields():
#     """Testa se um erro é levantado ao faltar campos obrigatórios."""
#     with pytest.raises(ValidationError, match="Os campos obrigatórios estão faltando: project_uuid, name"):
#         msg = Msg()  # Nenhum dado enviado
#         insert_data(MsgPath, msg)

# def test_insert_extra_fields():
#     """Testa se campos extras podem ser passados sem erro."""
#     msg = Msg(
#         project_uuid="123e4567-e89b-12d3-a456-426614174000",
#         name="Test",
#         extra_field="Isso não deveria causar erro"
#     )

#     # Não deve lançar erro, pois agora aceitamos campos extras
#     insert_data(MsgPath, msg)
