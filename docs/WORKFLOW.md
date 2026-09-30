# 日常操作手册

> 2026-09-30 整理。**记住一句话：改代码只在 `~/ecom-warehouse` 里改，其余全是它的入口。**

---

## 一、先记住三个路径

| 路径 | 是什么 | 怎么用 |
|---|---|---|
| `~/ecom-warehouse` | **唯一的工作目录**（真实文件 + 独立 git 仓库） | 改代码、提交、推送都在这里 |
| `~/my-spark/my-second-project-add` | → 软链接，指向 `~/ecom-warehouse` | 老习惯路径，照常用，改它就是改上面那个 |
| `~/my-spark` | 本地开发环境（HDFS 数据、venv、Airflow、启停脚本） | 环境和运维用，**代码不在这里** |

因为 `my-second-project-add` 是软链接，所以**下面两条命令完全等价**：

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

用**前缀 + 一句话**，方便以后 `git log` 扫读：

| 前缀 | 用在 |
|---|---|
| `feat:` | 新功能、新表 |
| `fix:` | 修 bug |
| `docs:` | 只改文档 |
| `refactor:` | 重构，行为不变 |
| `chore:` | 杂项（依赖、配置、清理） |

### 第一次推送（还没做！）

远端仓库建好了但内容还没上去，需要**先做这一次**：

```bash
cd ~/ecom-warehouse
git push -u origin main
```

会提示输入账号密码 —— 密码处填 **Personal Access Token**（GitHub 早就不支持账号密码了）。
`-u` 只需第一次，之后直接 `git push` 即可。

---

## 三、跑数仓任务

**统一从 `run.sh` 进**，不要直接 `python xxx.py`（绕过串行闸门会多个 SparkSession 抢内存）：

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

`run.sh` 会自动找解释器，顺序是：`PYTHON` 环境变量 → 仓库内 `.venv` → 同级 `.venv` →
`~/my-spark/.venv`。你的 venv 在 `~/my-spark/.venv`，**会自动匹配到，不用额外传参**。

需要指定别的解释器时才用：

```bash
PYTHON=/path/to/python ./run.sh build --dt 2019-11-01
```

**环境启停**（注意这两个在 my-spark，不在 ecom-warehouse）：

```bash
cd ~/my-spark
./start-env.sh              # 启动 MySQL → HDFS → Spark
./start-env.sh status       # 看状态
./stop-env.sh               # 停
```

顺序：**先 `start-env.sh`，再跑数仓任务**。

---

## 四、Airflow

两条链路：

```
~/my-spark/airflow/dags/incremental_warehouse_dag.py
   → 软链接 → ~/ecom-warehouse/scheduler/incremental_warehouse_dag.py   ← 实体在这

~/my-spark/airflow/dags/ecom_warehouse_dag.py   （第一版项目的真实文件）
```

**含义**：改 `~/ecom-warehouse/scheduler/incremental_warehouse_dag.py` 就是改 Airflow 的 DAG，
改完提交推送、Airflow 自动重新加载。一份代码两处用，这是好事。

启停：

```bash
cd ~/my-spark/airflow
./start-airflow.sh
./stop-airflow.sh
```

`airflow/` 下除了 `dags/` 都是运行时产物（logs、密码文件、pid），已被 gitignore，不用管。

---

## 五、从 Windows 侧操作 WSL 的正确姿势

如果你在 Windows 的终端里操作 WSL，**必须带 `-u lst`**：

```bash
wsl -d Ubuntu-22.04 -u lst -- bash -lc 'cd ~/ecom-warehouse && git status'
```

**不带 `-u lst` 会以 root 运行**，之后 git 会报一堆权限错（已踩过两次）：

- `fatal: detected dubious ownership in repository`
- `insufficient permission for adding an object to repository database`

万一又撞上，修一下属主：

```bash
wsl -d Ubuntu-22.04 -- bash -c 'sudo -n chown -R lst:lst ~/ecom-warehouse'
```

订阅提醒：
- 发行版名是 **`Ubuntu-22.04`**，不是 `Ubuntu`
- 内联复杂脚本时 `$VAR` 会被外层 shell 提前展开 → 写成 `.sh` 文件再执行

---

## 六、两个仓库的分工（别搞混）

| | `~/ecom-warehouse` | `~/my-spark` |
|---|---|---|
| 定位 | 项目代码，**要展示** | 本地开发环境 |
| 远端 | `wushi007/offline-data-warehouse` | **无**（本地环境不该有远端） |
| git 里有什么 | 61 个文件，代码+文档 | 只有 `.gitignore` |
| 你会改它吗 | ✅ 天天改 | ❌ 基本不动 |
| 提交推送 | ✅ 要 | ❌ 不需要 |

`~/my-spark` 里其余内容（`airflow/`、`docs/`、`start-env.sh`、`my_second_project/`、5.1G 的前置项目等）
都是**未跟踪**状态 —— 未跟踪只表示「没进版本库」，**文件都在、照常能用**，不需要清理。

---

## 七、当前状态

- [x] 首次推送完成，远端 `offline-data-warehouse` 已是最新
- [x] `scheduler/incremental_warehouse_dag.py`、`run.sh` 的本机绝对路径
      （`SPARK_HOME` / `HADOOP_HOME` / `PROJECT_ROOT`）已改为自动探测 + 环境变量覆盖
- [ ] 确认新流程顺手后，可删备份 `~/my-second-project-add.bak`（800K）

---

## 八、一句话总结

> **代码只在 `~/ecom-warehouse` 改，用 `run.sh` 跑任务，`start-env.sh` 管环境，
> 提交完 `git push`。老路径 `my-second-project-add` 是软链接，照常用不影响。**
