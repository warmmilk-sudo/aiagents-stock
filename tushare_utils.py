"""
Tushare helpers.

Provides a single initialization path that supports both newer
`ts.pro_api(server=...)` versions and older versions that require
overriding the internal HTTP URL after client creation.
"""

import os

from dotenv import load_dotenv


DEFAULT_TUSHARE_URL = "https://api.tushare.pro"


load_dotenv()


def create_tushare_pro(token=None, base_url=None):
    """
    Build a Tushare pro client with optional custom base URL.

    Returns:
        tuple: (pro_client, resolved_base_url)
    """
    token = token if token is not None else os.getenv("TUSHARE_TOKEN", "")
    base_url = base_url if base_url is not None else os.getenv("TUSHARE_URL", DEFAULT_TUSHARE_URL)

    if not token:
        return None, base_url

    import tushare as ts

    ts.set_token(token)

    try:
        pro = ts.pro_api(server=base_url)
    except TypeError:
        # Older tushare versions do not support the `server` argument.
        pro = ts.pro_api()
        if hasattr(pro, "_DataApi__http_url"):
            pro._DataApi__http_url = base_url

    return pro, base_url
