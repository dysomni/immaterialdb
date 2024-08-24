from typing import Any


def safe_dot_access(obj: Any, path, default=None):
    """
    Safely access nested attributes of an object.
    """
    try:
        for key in path.split("."):
            obj = obj[key]
        return obj
    except KeyError:
        return default
    except TypeError:
        return default
