from weni.utils.exceptions import ValidationError
from weni.contracts import Msg

VALID_CONTRACTS = {
    "messages": Msg,
}

def validate_path(path_class):
    """
    Validated if path is correct.
    """
    table_name = path_class.get_table_name()

    if table_name not in VALID_CONTRACTS:
        raise ValidationError(f"Path '{table_name}' is not valid.")
