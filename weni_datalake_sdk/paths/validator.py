from weni_datalake_sdk.paths.msg_path import MsgPath
from weni_datalake_sdk.utils.exceptions import ValidationError

VALID_CONTRACTS = {
    "messages": MsgPath,
}

def validate_path(path_class):
    """
    Validated if path is correct.
    """
    table_name = path_class.get_table_name()

    if table_name not in VALID_CONTRACTS:
        raise ValidationError(f"Path '{table_name}' is not valid.")
