import json
from typing import Any


def extract_operation_params(
    history: list[dict],
    param_key: str,
    operations: set[str] = {"OPTIMIZE", "CREATE TABLE"},
) -> Any:
    """
    Get operationParameters from history.
    """

    for commit in history:
        if commit.get("operation") in operations:
            param = commit.get("operationParameters", {}).get(param_key)
            if param: 
                return json.loads(param)
            else:
                return

