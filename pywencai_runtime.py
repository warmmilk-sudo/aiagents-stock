"""Runtime bootstrap for pywencai-related Node.js subprocess noise control."""

from __future__ import annotations

import os


def setup_pywencai_runtime_env() -> None:
    """Configure process env before importing pywencai.

    pywencai may spawn Node.js-based components that emit noisy deprecation
    warnings (e.g., `punycode` on newer Node versions). We suppress those
    warnings at process level.
    """

    os.environ.setdefault("NODE_NO_WARNINGS", "1")

    node_options = os.environ.get("NODE_OPTIONS", "").strip()
    if "--no-deprecation" not in node_options.split():
        os.environ["NODE_OPTIONS"] = f"{node_options} --no-deprecation".strip()
