"""Runtime bootstrap for pywencai-related subprocess noise control."""

from __future__ import annotations

import os
from typing import Any


def setup_pywencai_runtime_env() -> None:
    """Configure process env before importing pywencai.

    pywencai may spawn Node.js-based components that emit noisy deprecation
    warnings (e.g., `punycode` on newer Node versions). Some transitive
    dependencies also emit tqdm-style terminal progress bars. We suppress both
    at process level before pywencai is imported.
    """

    os.environ.setdefault("NODE_NO_WARNINGS", "1")
    os.environ.setdefault("TQDM_DISABLE", "1")

    node_options = os.environ.get("NODE_OPTIONS", "").strip()
    if "--no-deprecation" not in node_options.split():
        os.environ["NODE_OPTIONS"] = f"{node_options} --no-deprecation".strip()

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
        return
