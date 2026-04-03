from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Dict, List


PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


@lru_cache(maxsize=None)
def load_prompt_template(template_path: str) -> str:
    return (PROMPTS_DIR / template_path).read_text(encoding="utf-8").strip()


def render_prompt(template_path: str, **context: object) -> str:
    normalized_context = {key: "" if value is None else str(value) for key, value in context.items()}
    return Template(load_prompt_template(template_path)).safe_substitute(normalized_context)


def build_messages(system_template: str, user_template: str, **context: object) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": render_prompt(system_template, **context)},
        {"role": "user", "content": render_prompt(user_template, **context)},
    ]
