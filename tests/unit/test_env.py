import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from config.env import load_env, read_env

ROOT=Path(__file__).resolve().parents[2]


class EnvironmentTests(unittest.TestCase):
    def test_shell_and_python_preserve_external_values(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'env'
            path.write_text('VALUE=file\nEMPTY=file\nQUOTED="a b" # note\nLITERAL=\'$(touch should-not-exist)\'\n')
            env={**os.environ,'DW_ENV_FILE':str(path),'VALUE':'external','EMPTY':''}
            with patch.dict(os.environ,env,clear=True):
                load_env()
                self.assertEqual(os.environ['VALUE'],'external')
                self.assertEqual(os.environ['EMPTY'],'')
                self.assertEqual(os.environ['QUOTED'],'a b')
            result=subprocess.run(['bash','-c',
                'source scripts/env.sh; printf "%s|%s|%s|%s" "$VALUE" "$EMPTY" "$QUOTED" "$LITERAL"'],
                cwd=ROOT,env=env,capture_output=True,text=True,check=True)
            self.assertEqual(result.stdout,'external||a b|$(touch should-not-exist)')
            self.assertFalse((ROOT/'should-not-exist').exists())

    def test_hashes_and_quoted_comments_are_preserved(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'env'
            path.write_text('PASSWORD=abc#def\nQUOTED="#keep this" # comment\nEMPTY= # comment\n')
            self.assertEqual(read_env(path),{'PASSWORD':'abc#def','QUOTED':'#keep this','EMPTY':''})

    def test_invalid_line_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'env';path.write_text('not-valid=value\n')
            with self.assertRaises(ValueError):read_env(path)
