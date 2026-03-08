"""Global interpreter hooks for local runtime defaults.

This file is auto-imported by Python (if available on sys.path) during startup.
We use it to suppress noisy Node.js deprecation output from pywencai's subprocesses.
"""

from __future__ import annotations

import os


os.environ.setdefault("NODE_NO_WARNINGS", "1")

_node_options = os.environ.get("NODE_OPTIONS", "").strip()
if "--no-deprecation" not in _node_options.split():
    os.environ["NODE_OPTIONS"] = f"{_node_options} --no-deprecation".strip()
