"""打印可公开的组件版本和路径；不读取或输出密码。"""
import importlib.metadata
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]


def main():
    print(f'项目：{ROOT}\nPython：{sys.version.split()[0]}\n解释器：{sys.executable}')
    missing=[]
    for package in ('pyspark','streamlit','pymysql'):
        try:
            print(f'{package}：{importlib.metadata.version(package)}')
        except importlib.metadata.PackageNotFoundError:
            print(f'{package}：未安装'+('（可选）' if package!='pyspark' else '（核心）'))
            if package=='pyspark': missing.append(package)
    java=shutil.which('java')
    if java:
        result=subprocess.run([java,'-version'],capture_output=True,text=True)
        print('Java：'+(result.stderr or result.stdout).splitlines()[0])
    else:
        missing.append('Java');print('Java：未找到，请安装 JDK17')
    print('核心演示：bash scripts/demo.sh --verify-rerun')
    print('SPARK_HOME：'+os.environ.get('SPARK_HOME','未设置（演示使用 pip Spark）'))
    print('Airflow：独立环境，按 docs/operations.md 配置；核心演示不需要它')
    return int(bool(missing))


if __name__=='__main__':
    raise SystemExit(main())
