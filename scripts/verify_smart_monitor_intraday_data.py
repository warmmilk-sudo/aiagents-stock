#!/usr/bin/env python3
"""
Verify whether smart monitor intraday decision analysis received complete and
accurate data.

This script checks the intraday strict data contract used by
`SmartMonitorEngine.analyze_stock()`:

- quote data must come from TDX
- technical indicators must come from Tushare daily data
- key quote / technical fields must exist and be internally consistent

It does not call the DeepSeek API. The goal is to validate the input data
before any AI decision is made.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

REPO_VENV_DIR = ROOT_DIR / "venv"
REPO_VENV_PYTHON_CANDIDATES = (
    REPO_VENV_DIR / "bin" / "python3",
    REPO_VENV_DIR / "bin" / "python",
)


def _bootstrap_repo_venv() -> None:
    """Re-exec the script with the repository venv interpreter when available."""
    try:
        current_prefix = Path(sys.prefix).resolve()
    except Exception:
        current_prefix = Path(sys.prefix)

    if current_prefix == REPO_VENV_DIR.resolve():
        return

    for candidate in REPO_VENV_PYTHON_CANDIDATES:
        if candidate.exists() and os.access(candidate, os.X_OK):
            os.execv(str(candidate), [str(candidate), str(Path(__file__).resolve()), *sys.argv[1:]])
            return


_bootstrap_repo_venv()


STRICT_REQUIRED_FIELDS: Tuple[str, ...] = (
    "code",
    "name",
    "current_price",
    "change_pct",
    "change_amount",
    "high",
    "low",
    "open",
    "pre_close",
    "volume",
    "amount",
    "turnover_rate",
    "update_time",
    "ma5",
    "ma20",
    "ma60",
    "trend",
    "macd_dif",
    "macd_dea",
    "macd",
    "rsi6",
    "rsi12",
    "rsi24",
    "kdj_k",
    "kdj_d",
    "kdj_j",
    "boll_upper",
    "boll_mid",
    "boll_lower",
    "boll_position",
    "vol_ma5",
    "volume_ratio",
    "precision_status",
    "precision_mode",
    "data_source",
    "technical_data_source",
    "technical_period",
)

EXPECTED_STRICT_VALUES: Dict[str, str] = {
    "precision_status": "validated",
    "precision_mode": "tdx_quote_tushare_daily",
    "data_source": "tdx",
    "technical_data_source": "tushare",
    "technical_period": "daily",
}

ALLOWED_TRENDS = {"up", "down", "sideways"}


@dataclass
class FieldCheck:
    field: str
    status: str
    value: Any = None
    message: str = ""


@dataclass
class VerificationReport:
    stock_code: str
    stock_name: str = ""
    ok: bool = False
    strict_ok: bool = False
    errors: List[str] = None
    warnings: List[str] = None
    checks: List[FieldCheck] = None

    def to_json(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["checks"] = [asdict(item) for item in self.checks or []]
        payload["errors"] = list(self.errors or [])
        payload["warnings"] = list(self.warnings or [])
        return payload


def _safe_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _is_finite_number(value: Any) -> bool:
    number = _safe_float(value)
    return math.isfinite(number)


def _is_missing_value(value: Any) -> bool:
    return value in (None, "", [], {})


def _value_repr(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


def _check_required_fields(data: Dict[str, Any], required_fields: Sequence[str]) -> List[str]:
    missing = []
    for field in required_fields:
        if _is_missing_value(data.get(field)):
            missing.append(field)
    return missing


def _check_numeric_range(
    field: str,
    value: Any,
    errors: List[str],
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> None:
    if not _is_finite_number(value):
        errors.append(f"{field} 不是有效数字: {_value_repr(value)}")
        return

    number = float(value)
    if minimum is not None and number < minimum:
        errors.append(f"{field} 低于最小值 {minimum}: {number}")
    if maximum is not None and number > maximum:
        errors.append(f"{field} 高于最大值 {maximum}: {number}")


def _parse_update_time(text: Any) -> Optional[datetime]:
    value = str(text or "").strip()
    if not value:
        return None

    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    )
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def validate_intraday_market_data(stock_code: str, data: Dict[str, Any]) -> VerificationReport:
    errors: List[str] = []
    warnings: List[str] = []
    checks: List[FieldCheck] = []
    stock_name = str(data.get("name") or "").strip()

    if not isinstance(data, dict) or not data:
        return VerificationReport(
            stock_code=stock_code,
            ok=False,
            strict_ok=False,
            errors=["未获取到任何市场数据"],
            warnings=[],
            checks=[],
        )

    missing = _check_required_fields(data, STRICT_REQUIRED_FIELDS)
    for field in STRICT_REQUIRED_FIELDS:
        value = data.get(field)
        if field in missing:
            checks.append(FieldCheck(field=field, status="missing", value=None, message="字段缺失"))
        else:
            checks.append(FieldCheck(field=field, status="present", value=value, message=""))

    if stock_name in {"N/A", "-", "未知", "UNKNOWN"}:
        warnings.append("股票名称未能解析到有效值，prompt 会退化为代码展示")

    for field, expected_value in EXPECTED_STRICT_VALUES.items():
        actual_value = str(data.get(field) or "").strip()
        if actual_value != expected_value:
            errors.append(f"{field} 期望 {expected_value}，实际 {actual_value or '空值'}")

    if missing:
        errors.append("缺少严格模式必需字段: " + ", ".join(missing))

    numeric_fields = (
        "current_price",
        "change_pct",
        "change_amount",
        "high",
        "low",
        "open",
        "pre_close",
        "volume",
        "amount",
        "turnover_rate",
        "ma5",
        "ma20",
        "ma60",
        "macd_dif",
        "macd_dea",
        "macd",
        "rsi6",
        "rsi12",
        "rsi24",
        "kdj_k",
        "kdj_d",
        "kdj_j",
        "boll_upper",
        "boll_mid",
        "boll_lower",
        "vol_ma5",
        "volume_ratio",
    )

    for field in numeric_fields:
        _check_numeric_range(field, data.get(field), errors)

    current_price = _safe_float(data.get("current_price"))
    pre_close = _safe_float(data.get("pre_close"))
    open_price = _safe_float(data.get("open"))
    high_price = _safe_float(data.get("high"))
    low_price = _safe_float(data.get("low"))
    change_amount = _safe_float(data.get("change_amount"))
    change_pct = _safe_float(data.get("change_pct"))

    if math.isfinite(current_price) and current_price <= 0:
        errors.append(f"current_price 必须大于 0，实际为 {current_price}")
    if math.isfinite(pre_close) and pre_close <= 0:
        errors.append(f"pre_close 必须大于 0，实际为 {pre_close}")

    if all(math.isfinite(value) for value in (low_price, high_price, open_price, current_price, pre_close)):
        if low_price > high_price:
            errors.append(f"价格区间异常: low={low_price} > high={high_price}")
        if not (low_price <= open_price <= high_price):
            errors.append(f"开盘价不在日内区间内: open={open_price}, low={low_price}, high={high_price}")
        if not (low_price <= current_price <= high_price):
            errors.append(
                f"当前价不在日内区间内: current_price={current_price}, low={low_price}, high={high_price}"
            )

        expected_change_amount = current_price - pre_close
        expected_change_pct = (expected_change_amount / pre_close * 100.0) if pre_close else float("nan")
        if math.isfinite(change_amount) and abs(expected_change_amount - change_amount) > 0.05:
            errors.append(
                f"change_amount 与 current_price/pre_close 不一致: "
                f"expected={expected_change_amount:.4f}, actual={change_amount:.4f}"
            )
        if math.isfinite(change_pct) and abs(expected_change_pct - change_pct) > 0.15:
            errors.append(
                f"change_pct 与 current_price/pre_close 不一致: "
                f"expected={expected_change_pct:.4f}, actual={change_pct:.4f}"
            )

    if str(data.get("trend") or "").strip().lower() not in ALLOWED_TRENDS:
        errors.append(f"trend 不在允许集合内: {data.get('trend')}")

    if math.isfinite(_safe_float(data.get("rsi6"))) and not (0.0 <= _safe_float(data.get("rsi6")) <= 100.0):
        errors.append(f"rsi6 超出合理范围: {data.get('rsi6')}")
    if math.isfinite(_safe_float(data.get("rsi12"))) and not (0.0 <= _safe_float(data.get("rsi12")) <= 100.0):
        errors.append(f"rsi12 超出合理范围: {data.get('rsi12')}")
    if math.isfinite(_safe_float(data.get("rsi24"))) and not (0.0 <= _safe_float(data.get("rsi24")) <= 100.0):
        errors.append(f"rsi24 超出合理范围: {data.get('rsi24')}")

    boll_upper = _safe_float(data.get("boll_upper"))
    boll_mid = _safe_float(data.get("boll_mid"))
    boll_lower = _safe_float(data.get("boll_lower"))
    if all(math.isfinite(value) for value in (boll_lower, boll_mid, boll_upper)):
        if not (boll_lower <= boll_mid <= boll_upper):
            errors.append(
                f"布林带顺序异常: lower={boll_lower}, mid={boll_mid}, upper={boll_upper}"
            )

    if str(data.get("boll_position") or "").strip() == "":
        errors.append("boll_position 不能为空")

    update_time = _parse_update_time(data.get("update_time"))
    if update_time is None:
        errors.append(f"update_time 无法解析: {data.get('update_time')}")
    else:
        current_year = datetime.now().year
        if update_time.year < 2000 or update_time.year > current_year + 1:
            warnings.append(
                f"update_time 看起来异常: {update_time:%Y-%m-%d %H:%M:%S}，"
                "很可能是原始时间戳解析方式不正确"
            )

    if str(data.get("technical_period") or "").strip().lower() != "daily":
        errors.append(f"technical_period 必须为 daily，实际为 {data.get('technical_period')}")

    if str(data.get("technical_data_source") or "").strip().lower() != "tushare":
        errors.append(f"technical_data_source 必须为 tushare，实际为 {data.get('technical_data_source')}")

    if str(data.get("data_source") or "").strip().lower() != "tdx":
        errors.append(f"data_source 必须为 tdx，实际为 {data.get('data_source')}")

    if str(data.get("name") or "").strip() == "":
        errors.append("name 不能为空")

    if str(data.get("code") or "").strip() != stock_code:
        errors.append(f"code 与输入不一致: expected={stock_code}, actual={data.get('code')}")

    turnover_rate = _safe_float(data.get("turnover_rate"))
    if not math.isfinite(turnover_rate):
        errors.append("turnover_rate 不是有效数字或缺失")

    # Volume/amount can be 0 for suspended names or early session edge cases.
    # Warn instead of failing so the script remains usable across sessions.
    volume = _safe_float(data.get("volume"))
    amount = _safe_float(data.get("amount"))
    if math.isfinite(volume) and volume <= 0:
        warnings.append("volume 为 0 或缺失，占位/停牌/非活跃状态需要人工确认")
    if math.isfinite(amount) and amount <= 0:
        warnings.append("amount 为 0 或缺失，占位/停牌/非活跃状态需要人工确认")

    if str(data.get("volume_ratio") or "").strip() == "":
        errors.append("volume_ratio 不能为空")

    strict_ok = not errors
    ok = strict_ok and not warnings

    return VerificationReport(
        stock_code=stock_code,
        stock_name=stock_name,
        ok=ok,
        strict_ok=strict_ok,
        errors=errors,
        warnings=warnings,
        checks=checks,
    )


def _format_summary(report: VerificationReport, data: Dict[str, Any]) -> str:
    status = "PASS" if report.ok else ("PARTIAL" if report.strict_ok else "FAIL")
    name = report.stock_name or str(data.get("name") or report.stock_code)
    lines = [
        f"{report.stock_code} {name}: {status}",
        f"  precision_status={data.get('precision_status')}",
        f"  precision_mode={data.get('precision_mode')}",
        f"  data_source={data.get('data_source')}",
        f"  technical_data_source={data.get('technical_data_source')}",
        f"  technical_period={data.get('technical_period')}",
        (
            "  price="
            f"{data.get('current_price')} "
            f"change={data.get('change_pct')}% "
            f"volume={data.get('volume')} "
            f"amount={data.get('amount')}"
        ),
    ]

    if report.errors:
        lines.append("  errors:")
        lines.extend(f"    - {item}" for item in report.errors)
    if report.warnings:
        lines.append("  warnings:")
        lines.extend(f"    - {item}" for item in report.warnings)

    return "\n".join(lines)


def _iter_symbols(raw_values: Sequence[str]) -> List[str]:
    symbols: List[str] = []
    for raw in raw_values:
        for item in str(raw).split(","):
            symbol = item.strip()
            if symbol:
                symbols.append(symbol)
    return symbols


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify smart monitor intraday decision data completeness and accuracy.",
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        default=["600519"],
        help="Stock codes to verify, e.g. 600519 000001",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON only.",
    )
    args = parser.parse_args()

    symbols = _iter_symbols(args.symbols)
    try:
        from smart_monitor_data import SmartMonitorDataFetcher  # noqa: WPS433
    except ModuleNotFoundError as exc:
        print(
            "无法导入 smart_monitor_data 依赖模块，通常是因为缺少 pandas 等运行依赖。",
            file=sys.stderr,
        )
        print(f"原始错误: {exc}", file=sys.stderr)
        return 3

    fetcher = SmartMonitorDataFetcher()

    reports: List[Tuple[VerificationReport, Dict[str, Any]]] = []
    for symbol in symbols:
        data = fetcher.get_comprehensive_data(symbol, intraday_strict=True)
        report = validate_intraday_market_data(symbol, data or {})
        reports.append((report, data or {}))

    if args.json:
        payload = {
            "reports": [
                {
                    **report.to_json(),
                    "summary": _format_summary(report, data),
                }
                for report, data in reports
            ]
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for index, (report, data) in enumerate(reports):
            if index:
                print()
            print(_format_summary(report, data))

    has_errors = any(report.errors for report, _ in reports)
    has_warnings = any(report.warnings for report, _ in reports)
    if has_errors:
        return 1
    if has_warnings:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
