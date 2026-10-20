#!/usr/bin/env bash
# =============================================================================
# bench_bitmap_uv.sh —— Bitmap UV 去重基准（可复现）
# =============================================================================
# 用途：对照 COUNT(DISTINCT) 与 bitmap_union_count 的耗时，并校验结果一致。
# 环境：StarRocks 4.1.4（本机 docker 单 BE），连接 127.0.0.1:9030 root 空密码
# 方法：预热 1 次 + 计时 3 次，取最优/平均（与 bench_starrocks.py 口径一致）
# 用法：
#   bash starrocks/bench_bitmap_uv.sh                       # 只打印
#   bash starrocks/bench_bitmap_uv.sh --out                # 打印 + 落盘默认路径
#   bash starrocks/bench_bitmap_uv.sh --out /path/x.json   # 打印 + 指定路径
#   bash starrocks/bench_bitmap_uv.sh --rep 5              # 改重复次数（默认 3）
# 落盘格式与 bench_results/*.json 一致，便于归档比对。
#
# 依赖 dwd.dwd_user_uv_bitmap 已建表并灌数，语句见 bitmap_uv_dedup.sql 第 1~2 节。
# 逐日 UV 相加不等于区间 UV，区间去重须先 bitmap OR 再 count。
# =============================================================================
set -uo pipefail

HOST=127.0.0.1; PORT=9030; USER=root
REP=3
OUT=""

# ---- 参数解析 ----
OUT_GIVEN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --out)  OUT_GIVEN=1; OUT="${2:-}"; [ $# -ge 2 ] && shift ;;
    --rep)  REP="${2:-3}"; [ $# -ge 2 ] && shift ;;
    -h|--help)
      sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数: $1（用 --help 看用法）" >&2; exit 2 ;;
  esac
  shift
done
# --out 不带值时用默认路径（带日期，避免覆盖历史）
if [ "$OUT_GIVEN" = "1" ] && [ -z "$OUT" ]; then
  OUT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench_results/bitmap_uv_$(date +%Y%m%d).json"
fi
[ -n "$OUT" ] && mkdir -p "$(dirname "$OUT")"

# 结果收集（JSON 片段）
RESULTS=""

Q () { mysql -h"$HOST" -P"$PORT" -u"$USER" -N -B -e "$1" 2>&1; }

bench () {
  local label="$1" sql="$2"
  Q "$sql" >/dev/null 2>&1                       # 预热
  local best=999999999 avg=0 ms val t0 t1
  for i in $(seq 1 "$REP"); do
    t0=$(date +%s%N)
    val=$(Q "$sql")
    t1=$(date +%s%N)
    ms=$(( (t1 - t0) / 1000000 ))
    echo "      run$i: ${ms} ms  -> ${val}"
    [ "$ms" -lt "$best" ] && best=$ms
    avg=$(( avg + ms ))
  done
  avg=$(( avg / REP ))
  echo "    ==> ${label}  最优 ${best} ms / 平均 ${avg} ms"
  echo
  # 累加 JSON 片段，转义引号与反斜杠
  local esc_val; esc_val=$(printf '%s' "$val" | sed 's/\\/\\\\/g; s/"/\\"/g')
  RESULTS="${RESULTS}${RESULTS:+,}
    {\"label\": \"${label}\", \"reps\": ${REP}, \"best_ms\": ${best}, \"avg_ms\": ${avg}, \"value\": \"${esc_val}\"}"
}

echo "=============================================================="
echo "环境"
echo "=============================================================="
Q "SELECT current_version();" | sed 's/^/  StarRocks /'
Q "SELECT COUNT(*) FROM dwd.dwd_event_fact;"   | sed 's/^/  dwd_event_fact 行数: /'
Q "SELECT COUNT(*) FROM dwd.dwd_user_uv_bitmap;" | sed 's/^/  dwd_user_uv_bitmap 行数: /'
echo

echo "=============================================================="
echo "1) 全量 UV：COUNT(DISTINCT) vs bitmap_union_count"
echo "=============================================================="
bench "COUNT(DISTINCT) 全量" \
      "SELECT COUNT(DISTINCT user_id) FROM dwd.dwd_event_fact;"
bench "bitmap_union_count 全量" \
      "SELECT bitmap_union_count(user_bitmap) FROM dwd.dwd_user_uv_bitmap;"

echo "=============================================================="
echo "2) 按天 UV 分组（61 组）—— 只测「分组耗时」，SUM 仅用于消耗结果"
echo "=============================================================="
echo "  WARN: 注意：SUM(c) 是把 61 天的 UV 相加，语义上不是「区间 UV」"
echo "     （区间 UV 必须 bitmaps OR 后再 count，见第 4 节）。"
echo "     这里用 SUM 只是为了强制引擎把 61 组全算出来、避免惰性求值。"
bench "COUNT(DISTINCT) 按天" \
      "SELECT SUM(c) FROM (SELECT event_date, COUNT(DISTINCT user_id) c FROM dwd.dwd_event_fact GROUP BY event_date) t;"
bench "bitmap 按天" \
      "SELECT SUM(c) FROM (SELECT event_date, bitmap_union_count(user_bitmap) c FROM dwd.dwd_user_uv_bitmap GROUP BY event_date) t;"

echo "=============================================================="
echo "3) 正确性校验（必须逐值一致）"
echo "=============================================================="
echo "  全量 UV:"
echo "    COUNT(DISTINCT) : $(Q 'SELECT COUNT(DISTINCT user_id) FROM dwd.dwd_event_fact;')"
echo "    bitmap          : $(Q 'SELECT bitmap_union_count(user_bitmap) FROM dwd.dwd_user_uv_bitmap;')"
echo "  月度 UV:"
Q "SELECT CONCAT('    ', DATE_FORMAT(event_date,'%Y-%m'), '  bitmap=', bitmap_union_count(user_bitmap))
   FROM dwd.dwd_user_uv_bitmap GROUP BY DATE_FORMAT(event_date,'%Y-%m') ORDER BY 1;"
Q "SELECT CONCAT('    ', DATE_FORMAT(event_date,'%Y-%m'), '  cntdis=', COUNT(DISTINCT user_id))
   FROM dwd.dwd_event_fact GROUP BY DATE_FORMAT(event_date,'%Y-%m') ORDER BY 1;"

echo
echo "=============================================================="
echo "4) 语义陷阱验证：逐日 UV 相加 ≠ 区间 UV"
echo "=============================================================="
echo "  SUM(bitmap_count)      = $(Q 'SELECT SUM(bitmap_count(user_bitmap)) FROM dwd.dwd_user_uv_bitmap;')"
echo "  bitmap_union_count     = $(Q 'SELECT bitmap_union_count(user_bitmap) FROM dwd.dwd_user_uv_bitmap;')"
echo "  ← 两者不等，因为同一用户多天活跃会被重复计数"

# ---- 落盘（--out 时才写）----
if [ -n "$OUT" ]; then
  SR_VER=$(Q "SELECT current_version();")
  ROWS_FACT=$(Q "SELECT COUNT(*) FROM dwd.dwd_event_fact;")
  ROWS_BM=$(Q "SELECT COUNT(*) FROM dwd.dwd_user_uv_bitmap;")
  UV_CD=$(Q "SELECT COUNT(DISTINCT user_id) FROM dwd.dwd_event_fact;")
  UV_BM=$(Q "SELECT bitmap_union_count(user_bitmap) FROM dwd.dwd_user_uv_bitmap;")
  cat > "$OUT" <<JSON
{
  "meta": {
    "script": "starrocks/bench_bitmap_uv.sh",
    "generated_at": "$(date -Iseconds)",
    "reps": ${REP},
    "engine": "StarRocks ${SR_VER}",
    "endpoint": "${HOST}:${PORT}"
  },
  "data": {
    "dwd_event_fact_rows": ${ROWS_FACT},
    "dwd_user_uv_bitmap_rows": ${ROWS_BM}
  },
  "correctness": {
    "uv_count_distinct": ${UV_CD},
    "uv_bitmap": ${UV_BM},
    "consistent": $([ "$UV_CD" = "$UV_BM" ] && echo true || echo false)
  },
  "benchmarks": [${RESULTS}
  ]
}
JSON
  echo
  echo "OK: 结果已落盘: $OUT"
fi
