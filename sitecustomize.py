"""Global interpreter hooks for local runtime defaults.

This file is auto-imported by Python (if available on sys.path) during startup.
We use it to suppress noisy subprocess output, including Node.js deprecation
warnings from pywencai and tqdm-style terminal progress bars from dependencies.
"""

from __future__ import annotations

import os
from typing import Any


os.environ.setdefault("NODE_NO_WARNINGS", "1")
os.environ.setdefault("TQDM_DISABLE", "1")

_node_options = os.environ.get("NODE_OPTIONS", "").strip()
if "--no-deprecation" not in _node_options.split():
    os.environ["NODE_OPTIONS"] = f"{_node_options} --no-deprecation".strip()


def _patch_tqdm() -> None:
    """Force-disable terminal progress bars emitted by third-party libraries."""

    try:
        import importlib

        tqdm_module = importlib.import_module("tqdm")
        if getattr(tqdm_module, "_quiet_progress_patched", False):
            return

        original_tqdm = tqdm_module.tqdm

        class QuietTqdm(original_tqdm):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                kwargs["disable"] = True
                kwargs.setdefault("leave", False)
                super().__init__(*args, **kwargs)

        def quiet_trange(*args: Any, **kwargs: Any) -> QuietTqdm:
            return QuietTqdm(range(*args), **kwargs)

        tqdm_module.tqdm = QuietTqdm
        tqdm_module.trange = quiet_trange
        tqdm_module._quiet_progress_patched = True

        for module_name in ("tqdm.auto", "tqdm.std", "tqdm.autonotebook"):
            try:
                submodule = importlib.import_module(module_name)
            except Exception:
                continue
            setattr(submodule, "tqdm", QuietTqdm)
            setattr(submodule, "trange", quiet_trange)
    except Exception:
        # Best-effort only: startup should not fail if tqdm is unavailable.
        return


_patch_tqdm()
