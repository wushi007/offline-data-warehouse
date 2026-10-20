# 日常操作手册

> 2026-09-30 整理。代码只在 `~/ecom-warehouse` 中修改，其余路径都是它的入口。

---

## 一、先记住三个路径

| 路径 | 是什么 | 怎么用 |
|---|---|---|
| `~/ecom-warehouse` | 唯一的工作目录（真实文件 + 独立 git 仓库） | 改代码、提交、推送都在这里 |
| `~/my-spark/my-second-project-add` | 软链接，指向 `~/ecom-warehouse` | 旧路径，可继续使用，修改效果同上 |
| `~/my-spark` | 本地开发环境（HDFS 数据、venv、Airflow、启停脚本） | 用于环境和运维，不放代码 |

`my-second-project-add` 是软链接，下面两条命令等价：

```bash
cd ~/ecom-warehouse                        # 推荐（少一层解析）
cd ~/my-spark/my-second-project-add        # 也完全没问题
```

---

## 二、日常写代码 → 提交 → 推送

### 标准四步

```bash
cd ~/ecom-warehouse

# 1. 看改了什么
git status
git diff

# 2. 提交
git add -A
git commit -m "fix: 修了留存表右删失的 NULL 判断"

# 3. 推送
git push
```

### 关于提交信息

提交信息使用前缀 + 简短描述，便于 `git log` 扫读：

| 前缀 | 用在 |
|---|---|
| `feat:` | 新功能、新表 |
| `fix:` | 修 bug |
| `docs:` | 只改文档 |
| `refactor:` | 重构，行为不变 |
| `chore:` | 杂项（依赖、配置、清理） |

---

## 三、跑数仓任务

统一从 `run.sh` 进入，不要直接执行 `python xxx.py`，绕过串行闸门会导致多个 SparkSession 争抢内存：

```bash
cd ~/ecom-warehouse

./run.sh ddl                                    # 建库建表（首次）
./run.sh build --start 2019-10-01 --end 2019-10-31   # 全链路 + 对账
./run.sh build --dt 2019-11-01                  # 单日增量
./run.sh check --start 2019-10-01 --end 2019-10-31   # 只对账

# 单张表（调试用，一表一进程）
./run.sh dwd --dt 2019-11-01
./run.sh dim-scd2 --dt 2019-11-01
./run.sh ads --dt 2019-11-01
```

`run.sh` 按以下顺序查找解释器：`PYTHON` 环境变量 → 仓库内 `.venv` → 同级 `.venv` →
`~/my-spark/.venv`。venv 位于 `~/my-spark/.venv` 时会自动匹配，无需额外传参。

需要指定其他解释器时使用：

```bash
PYTHON=/path/to/python ./run.sh build --dt 2019-11-01
```

环境启停（两个脚本位于 my-spark，不在 ecom-warehouse）：

```bash
cd ~/my-spark
./start-env.sh              # 启动 MySQL → HDFS → Spark
./start-env.sh status       # 看状态
./stop-env.sh               # 停
```

执行顺序：先运行 `start-env.sh`，再跑数仓任务。

---

## 四、Airflow

Airflow 有两条链路：

```
~/my-spark/airflow/dags/incremental_warehouse_dag.py
   → 软链接 → ~/ecom-warehouse/scheduler/incremental_warehouse_dag.py   ← 实体在这

~/my-spark/airflow/dags/ecom_warehouse_dag.py   （第一版项目的真实文件）
```

修改 `~/ecom-warehouse/scheduler/incremental_warehouse_dag.py` 即修改 Airflow 的 DAG，
提交推送后 Airflow 自动重新加载，同一份代码在两处使用。

启停：

```bash
cd ~/my-spark/airflow
./start-airflow.sh
./stop-airflow.sh
```

`airflow/` 下除 `dags/` 外均为运行时产物（logs、密码文件、pid），已被 gitignore，无需处理。

---

## 五、从 Windows 侧操作 WSL 的正确姿势

在 Windows 终端中操作 WSL 时必须带 `-u lst`：

```bash
wsl -d Ubuntu-22.04 -u lst -- bash -lc 'cd ~/ecom-warehouse && git status'
```

不带 `-u lst` 会以 root 运行，之后 git 会报权限错误（已遇到两次）：

- `fatal: detected dubious ownership in repository`
- `insufficient permission for adding an object to repository database`

如果再次遇到，修复属主：

```bash
wsl -d Ubuntu-22.04 -- bash -c 'sudo -n chown -R lst:lst ~/ecom-warehouse'
```

注意事项：
- 发行版名是 `Ubuntu-22.04`，不是 `Ubuntu`
- 内联复杂脚本时 `$VAR` 会被外层 shell 提前展开，应写成 `.sh` 文件再执行

---

## 六、两个仓库的分工（别搞混）

| | `~/ecom-warehouse` | `~/my-spark` |
|---|---|---|
| 定位 | 项目代码，用于展示 | 本地开发环境 |
| 远端 | `wushi007/offline-data-warehouse` | 无（本地环境不需要远端） |
| git 里有什么 | 61 个文件，代码+文档 | 只有 `.gitignore` |
| 会修改它吗 | 经常修改 | 基本不动 |
| 提交推送 | 需要 | 不需要 |

`~/my-spark` 里的其余内容（`airflow/`、`docs/`、`start-env.sh`、`my_second_project/`、5.1G 的前置项目等）
均为未跟踪状态。未跟踪只表示没有加入版本库，文件仍在且可正常使用，不需要清理。

---

## 七、当前状态

- [x] 首次推送完成，远端 `offline-data-warehouse` 已是最新
- [x] `scheduler/incremental_warehouse_dag.py`、`run.sh` 的本机绝对路径
      （`SPARK_HOME` / `HADOOP_HOME` / `PROJECT_ROOT`）已改为自动探测 + 环境变量覆盖
- [ ] 确认新流程可用后，可删除备份 `~/my-second-project-add.bak`（800K）

---

## 八、总结

> 代码只在 `~/ecom-warehouse` 修改，用 `run.sh` 跑任务，`start-env.sh` 管理环境，
> 提交后 `git push`。旧路径 `my-second-project-add` 是软链接，可继续使用。
