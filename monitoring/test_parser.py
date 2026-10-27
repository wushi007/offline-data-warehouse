import json
import unittest
from parser import parse_errors


class ParserTests(unittest.TestCase):
    def test_traceback_type_and_code_are_one_error(self):
        text = '''Traceback (most recent call last):
  File "/repo/etl/orders.py", line 39, in main
pyspark.errors.ParseException:
[PARSE_SYNTAX_ERROR] Syntax error (line 2, pos 0)
== SQL ==
SELECT x
FRM orders
^^^
WHERE x > 0
Task failed with exception'''
        errors = parse_errors(text)
        self.assertEqual(len(errors), 1)
        e = errors[0]
        self.assertEqual(e['source_line'], 39)
        self.assertEqual(e['sql_line'], 2)
        self.assertIn('WHERE x > 0', e['sql'])

    def test_no_traceback_and_stderr_noise(self):
        self.assertEqual(parse_errors('[TRANSFORM] SQL\nWARN NativeCodeLoader: no library'), [])
        errors = parse_errors('ParseException:\n[PARSE_SYNTAX_ERROR] bad syntax')
        self.assertEqual(len(errors), 1)

    def test_structured_airflow_exception(self):
        row = {'event': 'Task failed with exception', 'error_detail': [{
            'exc_type':'AnalysisException','exc_value':'[UNRESOLVED_COLUMN.WITH_SUGGESTION] missing field',
            'frames':[{'filename':'/repo/etl/a.py','lineno':12}]}]}
        errors = parse_errors(json.dumps(row))
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['exception_type'],'AnalysisException')
        self.assertEqual(errors[0]['source_line'],12)
        self.assertEqual(errors[0]['error_code'],'UNRESOLVED_COLUMN.WITH_SUGGESTION')

    def test_merge_execution_error(self):
        errors=parse_errors(json.dumps({'event':'[MERGE_CARDINALITY_VIOLATION] Multiple source rows'}))
        self.assertEqual(errors[0]['category'],'执行错误')
        self.assertEqual(errors[0]['error_code'],'MERGE_CARDINALITY_VIOLATION')


if __name__ == '__main__':
    unittest.main()
