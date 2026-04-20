#!/usr/bin/env python3
"""Quick availability check for gpt-5.2.

Usage:
  OPENAI_API_KEY=... python3 scripts/test_gpt52_availability.py
  python3 scripts/test_gpt52_availability.py --model gpt-5.2 --prompt "hello"
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from openai import OpenAI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Test whether gpt-5.2 is currently callable.")
    parser.add_argument("--model", default="gpt-5.2", help="Model name to test (default: gpt-5.2).")
    parser.add_argument(
        "--prompt",
        default="Reply with exactly: OK",
        help="Tiny prompt used for the availability test.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("OPENAI_BASE_URL", "http://llmapi.warmmilk.fun/v1"),
        help="OpenAI API base URL (default: OPENAI_BASE_URL or the public OpenAI endpoint).",
    )
    parser.add_argument(
        "--api-key-env",
        default="OPENAI_API_KEY",
        help="Environment variable name that contains the API key (default: OPENAI_API_KEY).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="Sampling temperature for the probe request.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=32,
        help="Upper bound for the probe response length.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    api_key = os.getenv(args.api_key_env, "").strip()
    if not api_key:
        print(
            f"Missing API key: set {args.api_key_env} or pass a different --api-key-env.",
            file=sys.stderr,
        )
        return 2

    client = OpenAI(api_key=api_key, base_url=args.base_url)
    started_at = time.perf_counter()

    try:
        response = client.chat.completions.create(
            model=args.model,
            messages=[
                {"role": "system", "content": "You are a minimal connectivity test."},
                {"role": "user", "content": args.prompt},
            ],
            temperature=args.temperature,
            max_tokens=args.max_tokens,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        print(f"FAILED after {elapsed:.2f}s", file=sys.stderr)
        print(f"model={args.model}", file=sys.stderr)
        print(f"base_url={args.base_url}", file=sys.stderr)
        print(f"error={exc}", file=sys.stderr)
        return 1

    elapsed = time.perf_counter() - started_at
    content = response.choices[0].message.content if response.choices else ""
    print(f"OK in {elapsed:.2f}s")
    print(f"model={args.model}")
    print(f"base_url={args.base_url}")
    print(f"response={content!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
