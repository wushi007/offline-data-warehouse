"""将 Airflow JSONL 或纯文本日志中的 SQL 异常整理成结构化错误。"""
import json
import re

PARSER_VERSION = '3'

CODE = re.compile(r'\[([A-Z][A-Z0-9_.]+)\]')
LOCATION = re.compile(r'\(line (\d+), pos (\d+)\)')
FILE = re.compile(r'File "([^"]+\.py)", line (\d+)')
SQL_TYPES = ('ParseException', 'AnalysisException', 'SparkRuntimeException',
             'SparkArithmeticException', 'SparkNumberFormatException',
             'SparkDateTimeException', 'SparkSQLException')


def decode_log(raw):
    lines = []
    for line in raw.splitlines():
        item = None
        try:
            item = json.loads(line)
            msg = item.get('event', item.get('message', line)) if isinstance(item, dict) else line
            if not isinstance(msg, str):
                msg = str(msg)
        except (ValueError, TypeError):
            msg = line
        lines.extend(msg.splitlines() or [''])
        if isinstance(item, dict):
            def exception_lines(value):
                if isinstance(value, list):
                    return [line for entry in value for line in exception_lines(entry)]
                if isinstance(value, dict):
                    out = ['Traceback (most recent call last):']
                    for frame in value.get('frames', []):
                        out.append(f'  File "{frame.get("filename", "?")}", line {frame.get("lineno", 0)}')
                    out.append(f'{value.get("exc_type", "Exception")}: {value.get("exc_value", "")}')
                    out.extend(exception_lines(value.get('exceptions', [])))
                    return out
                return str(value).splitlines() if value else []
            for field in ('error_detail', 'exception', 'exc_info'):
                if item.get(field):
                    lines.extend(exception_lines(item[field]))
        item = None
    return lines


def parse_errors(raw):
    lines = decode_log(raw)
    found = []
    for i, line in enumerate(lines):
        code_match = CODE.search(line)
        code = code_match.group(1) if code_match else None
        # 不依据 ERROR 日志级别判断；Spark stderr 中也有普通警告。
        typed = any(t in line for t in SQL_TYPES)
        if not typed and not code:
            continue
        # 排除阶段标记、普通 WARN/INFO 等；只接受错误码或已知 SQL 异常。
        sql_code = bool(code and (code.startswith(('PARSE_', 'UNRESOLVED_', 'TABLE_OR_VIEW_',
                        'COLUMN_', 'MERGE_', 'DATATYPE_', 'INCOMPATIBLE_', 'INVALID_',
                        'DIVIDE_', 'CAST_', 'NUMERIC_', 'ARITHMETIC_', 'AMBIGUOUS_',
                        'MISSING_', 'UNSUPPORTED_', 'PATH_NOT_', 'SCHEMA_'))))
        if not typed and not sql_code:
            continue
        start = i
        for j in range(i - 1, max(-1, i - 100), -1):
            if any(t in lines[j] for t in SQL_TYPES):
                start = j
            if 'Traceback (most recent call last):' in lines[j]:
                start = j
                break
            if 'Task failed with exception' in lines[j] or '[DONE]' in lines[j]:
                break
        end = i + 1
        while end < len(lines):
            if any(x in lines[end] for x in ('Task failed with exception', '::group::',
                                             '::endgroup::', 'Traceback (most recent call last):')):
                break
            end += 1
        block = '\n'.join(lines[start:end])
        # 异常类型行与下一行错误码属于同一异常，合并而非重复统计。
        codes = [m.group(1) for m in CODE.finditer(block)
                 if m.group(1) not in ('SQL', 'ERROR', 'WARN', 'INFO')]
        actual_code = code or next((c for c in codes if c not in ('EXTRACT','TRANSFORM','LOAD')), None)
        if any(e['raw_traceback'] == block for e in found):
            continue
        match = LOCATION.search(block)
        files = FILE.findall(block)
        project_files = [(p, n) for p, n in files if '/site-packages/' not in p]
        source = project_files[-1] if project_files else (files[0] if files else (None, None))
        sql = None
        if '== SQL ==' in block:
            sql_lines = block.split('== SQL ==', 1)[1].splitlines()
            collected = []
            for s in sql_lines:
                if '^^^' in s:
                    continue
                if s.startswith(('== ', 'Caused by:', 'During handling', 'The above exception')):
                    break
                collected.append(s)
            sql = '\n'.join(collected).strip()
        error_line = next((s for s in lines[i:end] if actual_code and f'[{actual_code}]' in s), line)
        exception_type = next((t for t in SQL_TYPES if t in block), 'SparkSQLExecutionError')
        found.append(dict(error_code=actual_code, exception_type=exception_type,
            category='语法错误' if exception_type == 'ParseException' or (actual_code and actual_code.startswith('PARSE_')) else
                     '分析错误' if exception_type == 'AnalysisException' else '执行错误',
            message=error_line.strip(), source_file=source[0],
            source_line=int(source[1]) if source[1] else None,
            sql_line=int(match.group(1)) if match else None,
            sql_pos=int(match.group(2)) if match else None,
            sql=sql, raw_traceback=block))
    return found
