import re
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Dict, List


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


@lru_cache(maxsize=None)
def load_prompt_template(template_path: str) -> str:
    return (PROMPTS_DIR / template_path).read_text(encoding="utf-8").strip()


_EMPTY_VALUE_LINE_RE = re.compile(r"^[^:\n]+:\s*$")


def _compact_rendered_prompt(rendered: str) -> str:
    compacted: List[str] = []
    last_blank = True
    for raw_line in rendered.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            if compacted and not last_blank:
                compacted.append("")
            last_blank = True
            continue
        if _EMPTY_VALUE_LINE_RE.match(line.strip()):
            continue
        compacted.append(line)
        last_blank = False
    while compacted and not compacted[-1].strip():
        compacted.pop()
    return "\n".join(compacted).strip()


def render_prompt(template_path: str, *, omit_empty_lines: bool = False, **context: object) -> str:
    normalized_context = {key: "" if value is None else str(value) for key, value in context.items()}
    rendered = Template(load_prompt_template(template_path)).safe_substitute(normalized_context)
    if omit_empty_lines:
        return _compact_rendered_prompt(rendered)
    return rendered


def build_messages(system_template: str, user_template: str, **context: object) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": render_prompt(system_template, **context)},
        {"role": "user", "content": render_prompt(user_template, **context)},
    ]
