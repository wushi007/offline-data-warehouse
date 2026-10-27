import os
from pathlib import Path
import subprocess
import sys
import unittest

ROOT=Path(__file__).resolve().parents[2]


class CatalogTests(unittest.TestCase):
    def test_database_prefix_applies_to_create_and_repair(self):
        # 子进程隔离模块级配置，避免污染其它测试或读取本机 .env。
        code='''
from config.config import DBS
from etl.ddl import create_all, repair_partitions, ddl_statements
class Session:
 def __init__(self): self.statements=[]
 def sql(self,s): self.statements.append(s)
s=Session();create_all(s);repair_partitions(s,ddl_statements())
assert len(ddl_statements())==19
assert all("test_" in q for q in s.statements)
assert set(DBS.values())=={"test_"+x for x in ["ods","dwd","dim","dws","ads"]}
'''
        subprocess.run([sys.executable,'-c',code],cwd=ROOT,
            env={**os.environ,'DW_ENV_FILE':'/dev/null','DW_DB_PREFIX':'test_'},
            capture_output=True,text=True,check=True)
