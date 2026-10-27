# 开发与发布

在仓库根目录修改代码。已有 `~/my-spark/my-second-project-add` 仅是本机软链接，不是项目要求。

```bash
git status --short
bash scripts/test.sh
# 修改 ETL、配置或 DDL 后运行真实样本
bash scripts/demo.sh --verify-rerun
git diff --check
```

先建立分支，按依赖/行为/文档分开评审。不要使用 git add -A 把调试文件、个人材料和日志一并提交。

仓库同时配置 GitHub(origin) 和 Gitee(gitee)，历史 main 上游为 gitee/main。发布时显式指定目标，例如 `git push origin <branch>`，确认 PR 合并对象。无需重写已经公开的历史。

提交前检查：固定 fixture 纳入 Git；.env、完整日志、SQLite、数据文件未纳入；README 中的结果能找到对应命令或历史报告。

展示整理使用 chore/showcase-readiness 分支评审，不直接覆盖 main。
