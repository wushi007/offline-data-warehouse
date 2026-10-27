"""读取简单 dotenv 文件：外部环境变量 > dotenv > 代码默认值。不执行 shell。"""
import os
from pathlib import Path
import re
import shlex

ENV_FILE = Path(__file__).resolve().parent.parent / '.env'


def read_env(path=None):
    selected = path if path is not None else os.environ.get('DW_ENV_FILE', ENV_FILE)
    p = Path(selected).expanduser()
    if not p.is_file():
        return {}
    result = {}
    for number, line in enumerate(p.read_text(encoding='utf-8').splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:].strip()
        key, sep, value = line.partition('=')
        key = key.strip()
        if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key):
            raise ValueError(f'{p}:{number}: 非法环境变量定义')
        # shell 中 abc#def 是值，只有空白后开始的 # 才是行尾注释。
        quote = None
        escaped = False
        for i, char in enumerate(value):
            if escaped:
                escaped = False
                continue
            if char == "\\" and quote != "'":
                escaped = True
            elif char in ("'", '"'):
                quote = None if quote == char else char if quote is None else quote
            elif char == '#' and quote is None and (i == 0 or value[i-1].isspace()):
                value = value[:i]
                break
        parts = shlex.split(value, comments=False, posix=True)
        if len(parts) > 1:
            raise ValueError(f'{p}:{number}: 包含空格的值需要引号')
        result[key] = parts[0] if parts else ''
    return result


def load_env(path=None):
    for key, value in read_env(path).items():
        os.environ.setdefault(key, value)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--shell', action='store_true', required=True)
    parser.parse_args()
    for key, value in read_env().items():
        if key not in os.environ:
            print(f'export {key}={shlex.quote(value)}')
