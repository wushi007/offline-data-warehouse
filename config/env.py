# -*- coding: utf-8 -*-
"""加载项目根目录 .env 到环境变量。

.env 是敏感配置的唯一存放处，已被 .gitignore 忽略。
已存在的环境变量优先，不会被 .env 覆盖。
"""
import os
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def load_env(path=None):
    p = Path(path) if path else ENV_FILE
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


load_env()
