#!/usr/bin/env bash
# 被 bash source：只导入尚未设置的值，与 config.env.load_env 同一套规则。
_ENV_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
_ENV_EXPORTS=$(python3 "$_ENV_ROOT/config/env.py" --shell) || return 1
eval "$_ENV_EXPORTS"
unset _ENV_EXPORTS _ENV_ROOT
