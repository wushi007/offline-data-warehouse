# 从小样本开始

先确认 Python 3.10 和 JDK17：

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/doctor.py
bash scripts/test.sh
bash scripts/demo.sh --verify-rerun
```

这条链路不需要 HDFS、MySQL、Airflow 或原始大文件，使用独立本地目录和真实五层转换。
输出末尾的 passed 必须为 true，verification.json 记录每项检查。它验证结果，不测性能。

正式环境、Airflow 和运行台安装见 [运行手册](operations.md)，依赖拆分见 [requirements](../requirements/README.md)。
不要直接使用其它机器的绝对路径、虚拟环境软链接或服务密码。
