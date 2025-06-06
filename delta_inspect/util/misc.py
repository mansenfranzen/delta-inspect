from datetime import datetime


def to_datetime(unix_timestamp_ms: int) -> datetime:
    """
    Convert a Unix timestamp to a human-readable datetime string.

    Args:
        unix_timestamp (float): The Unix timestamp to convert.

    Returns:
        str: The formatted datetime string in 'YYYY-MM-DD HH:MM:SS' format.
    """

    return datetime.fromtimestamp(unix_timestamp_ms / 1000)