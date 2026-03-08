#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple local process helper for starting/stopping the Streamlit app.

Usage:
  python stm.py start
  python stm.py stop
  python stm.py restart
  python stm.py status
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency
    psutil = None


ROOT_DIR = Path(__file__).resolve().parent
APP_NAME = "app.py"
DEFAULT_PORT = int(os.getenv("STREAMLIT_PORT", "8503"))
DEFAULT_HOST = os.getenv("STREAMLIT_HOST", "127.0.0.1")
DEFAULT_LOG = ROOT_DIR / "app.log"


def _find_streamlit_pid(app_name: str = APP_NAME) -> Optional[int]:
    if psutil is None:
        return None
    for proc in psutil.process_iter(["pid", "cmdline"]):
        cmd = proc.info.get("cmdline") or []
        if not cmd:
            continue
        cmd_str = " ".join(str(x) for x in cmd).lower()
        if "streamlit" in cmd_str and "run" in cmd_str and app_name.lower() in cmd_str:
            return int(proc.info["pid"])
    return None


def start(app_name: str = APP_NAME, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> int:
    existing_pid = _find_streamlit_pid(app_name)
    if existing_pid:
        print(f"[INFO] Streamlit is already running (pid={existing_pid})")
        return 0

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        app_name,
        "--server.port",
        str(port),
        "--server.address",
        host,
    ]
    DEFAULT_LOG.parent.mkdir(parents=True, exist_ok=True)
    log_fp = DEFAULT_LOG.open("a", encoding="utf-8")
    popen_kwargs = {
        "cwd": str(ROOT_DIR),
        "stdout": log_fp,
        "stderr": log_fp,
    }

    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:  # pragma: no cover - platform dependent
        popen_kwargs["preexec_fn"] = os.setsid

    subprocess.Popen(cmd, **popen_kwargs)
    time.sleep(1.0)
    current_pid = _find_streamlit_pid(app_name)
    if current_pid:
        print(f"[OK] Started Streamlit (pid={current_pid}) http://{host}:{port}")
        return 0

    print("[ERR] Failed to detect a started Streamlit process; check app.log")
    return 1


def stop(app_name: str = APP_NAME) -> int:
    pid = _find_streamlit_pid(app_name)
    if not pid:
        print("[INFO] Streamlit is not running")
        return 0

    try:
        if psutil is not None:
            proc = psutil.Process(pid)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except psutil.TimeoutExpired:
                proc.kill()
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception as exc:
        print(f"[ERR] Failed to stop process {pid}: {exc}")
        return 1

    print(f"[OK] Stopped Streamlit (pid={pid})")
    return 0


def status(app_name: str = APP_NAME) -> int:
    pid = _find_streamlit_pid(app_name)
    if pid:
        print(f"[OK] Streamlit running (pid={pid})")
    else:
        print("[INFO] Streamlit not running")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Streamlit process for this project.")
    parser.add_argument("action", choices=["start", "stop", "restart", "status"])
    parser.add_argument("--app", default=APP_NAME, help="App entry file, default: app.py")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Streamlit bind host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Streamlit port")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.action == "start":
        return start(app_name=args.app, host=args.host, port=args.port)
    if args.action == "stop":
        return stop(app_name=args.app)
    if args.action == "restart":
        rc = stop(app_name=args.app)
        if rc != 0:
            return rc
        return start(app_name=args.app, host=args.host, port=args.port)
    return status(app_name=args.app)


if __name__ == "__main__":
    raise SystemExit(main())
