import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]


class DagInstallTests(unittest.TestCase):
    def test_repeated_install_and_refuse_other_file(self):
        with tempfile.TemporaryDirectory() as folder:
            env={**os.environ,'AIRFLOW_HOME':folder}
            command=['bash',str(ROOT/'scripts/install_dags.sh'),'--with-examples']
            subprocess.run(command,env=env,capture_output=True,check=True)
            subprocess.run(command,env=env,capture_output=True,check=True)
            target=Path(folder)/'dags/incremental_warehouse_dag.py'
            self.assertEqual(target.resolve(),ROOT/'scheduler/incremental_warehouse_dag.py')
            target.unlink();target.write_text('unrelated dag')
            result=subprocess.run(command,env=env,capture_output=True)
            self.assertNotEqual(result.returncode,0)
            self.assertEqual(target.read_text(),'unrelated dag')
