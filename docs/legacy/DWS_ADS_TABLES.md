# DWS / ADS 层数据字典

> 数据区间：2019-10-01 ~ 2019-11-03（34 天），源明细 `dwd_event_fact` 46,908,364 行。
> 行数/样例从 **StarRocks**（库 `dwd`）实测；Spark 侧表结构与行数完全一致（同口径，已验证）。
> 四类事件枚举：`view` / `cart` / `remove_from_cart` / `purchase`；源 purchase 共 **809,238** 行，总 GMV **249,929,113.38**。

---

## 一、DWS 轻度汇总层（3 张宽表，均按 `event_date` 分区）

数据流：`dwd_event_fact` → 按天 GROUP BY → DWS。

### 1.1 `dws_user_session_daily` —— 日 × 用户 × 会话
行数 **10,278,076**（34 天所有 用户×会话 组合）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `user_id` | bigint | 用户ID |
| `user_session` | varchar(64) | 会话ID |
| `view_cnt` | bigint | 浏览次数 |
| `cart_cnt` | bigint | 加购次数 |
| `purchase_cnt` | bigint | 购买次数 |
| `remove_cart_cnt` | bigint | 移出购物车次数 |
| `session_duration_sec` | bigint | 会话时长 = `GREATEST(MAX(event_time)-MIN(event_time),0)`（秒） |

样例（2019-10-01）：
```
user_id=260013793  session=70d27bfa-…  view=23 cart=0 purchase=0 remove=0 时长=914s
user_id=293291933  session=85e3fda6-…  view=1  cart=0 purchase=0 remove=0 时长=0s
```

### 1.2 `dws_product_daily` —— 日 × 商品
行数 **2,521,056**（34 天 × 当日有行为的商品）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `product_id` | bigint | 商品ID |
| `category_id` | bigint | 商品类目（冗余自明细） |
| `brand` | varchar(64) | 品牌（冗余自明细） |
| `exposure_uv` | bigint | 曝光UV = `COUNT(DISTINCT user_id WHERE view)` |
| `cart_uv` | bigint | 加购UV |
| `purchase_uv` | bigint | 购买UV |
| `sales_cnt` | bigint | 销量 = 该商品 purchase 行数 |
| `sales_amount` | double | 销售额 = `SUM(price WHERE purchase)` |

样例（2019-10-01）：
```
product=1001588  cat=2053013555631882655  brand=meizu   exposure_uv=39  cart_uv=0  purchase_uv=1  sales=1  amount=128.30
product=1002099  cat=2053013555631882655  brand=samsung exposure_uv=576 cart_uv=0  purchase_uv=7  sales=7  amount=2,592.87
```

### 1.3 `dws_user_behavior_daily` —— 日 × 用户
行数 **7,169,053**（34 天 × 当日活跃用户）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `user_id` | bigint | 用户ID |
| `active_action_cnt` | bigint | 当日行为总数 = `COUNT(*)` |
| `purchase_amount` | double | 购买金额 = `SUM(price WHERE purchase)` |
| `purchase_product_cnt` | bigint | 购买商品数 = `COUNT(DISTINCT product_id WHERE purchase)` |
| `purchase_cnt` | bigint | 购买次数 = `SUM(purchase)` |

样例（2019-10-01）：
```
user_id=295655799  active=1  purchase_amount=0.0  product_cnt=0  purchase_cnt=0
```

**DWS 对账**（三张 purchase 计数守恒）：
- `dws_user_session_daily.purchase_cnt` 合计 == `dws_product_daily.sales_cnt` 合计
  == `dws_user_behavior_daily.purchase_cnt` 合计 == 源 purchase **809,238**
- 交叉守恒：`dws_product_daily.sales_amount` 合计 == `dws_user_behavior_daily.purchase_amount` 合计 == **249,929,113.38**

---

## 二、ADS 应用层（5 张报表表，4 张日报 + 1 张快照）

数据流：DWS → ADS（漏斗/RFM 回 DWD 精确去重）。

### 2.1 `ads_trade_daily` —— 交易日报（源 `dws_user_behavior_daily`）
行数 **34**（每天 1 行）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `gmv` | double | 销售额 = `SUM(purchase_amount)` |
| `order_cnt` | bigint | 订单数 = `SUM(purchase_cnt)` |
| `buyer_cnt` | bigint | 购买用户数 = `SUM(IF(purchase_cnt>0,1,0))` |
| `avg_order_value` | double | 客单价 = gmv / order_cnt |
| `arppu` | double | 付费用户人均 = gmv / buyer_cnt |

样例（最新 3 天）：
```
2019-11-03  gmv=6,656,920.09  order=22,145  buyer=16,550  AOV=300.61  arppu=402.23
2019-11-02  gmv=6,389,578.47  order=21,863  buyer=16,187  AOV=292.26  arppu=394.74
2019-11-01  gmv=6,949,402.19  order=22,457  buyer=16,372  AOV=309.45  arppu=424.47
```
全期合计：GMV **249,929,113.38**，订单 **809,238**。

### 2.2 `ads_conversion_funnel_daily` —— 转化漏斗（源 `dwd_event_fact` 精确去重 UV）
行数 **34**（每天 1 行）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `view_uv` / `cart_uv` / `purchase_uv` | bigint | 各环节 `COUNT(DISTINCT user_id)`，跨商品去重 |
| `view_to_cart_rate` | double | 浏览→加购 = cart_uv / view_uv |
| `view_to_purchase_rate` | double | 浏览→购买 = purchase_uv / view_uv |
| `cart_to_purchase_rate` | double | 加购→购买 = purchase_uv / cart_uv |

样例（2019-10-01）：
```
view_uv=190,037  cart_uv=8,771  purchase_uv=14,064
view→cart=4.6%  view→purchase=7.4%  cart→purchase=160.3%
```
> **关键洞察**：`cart_to_purchase_rate > 100%` 意味着大量用户**直接购买、绕过加购环节**（真实业务特征），因此漏斗不变量校验用 `view_uv >= cart_uv 且 view_uv >= purchase_uv`（浏览是最上游）。

### 2.3 `ads_product_hot_rank_daily` —— 商品热榜 Top100（源 `dws_product_daily`）
行数 **3,400**（34 天 × 100）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `rank` | int | 当日排名 = `ROW_NUMBER() OVER (ORDER BY sales_amount DESC, sales_cnt DESC)` |
| `product_id` / `category_id` / `brand` | | 商品维度（含品牌） |
| `sales_cnt` / `sales_amount` / `exposure_uv` | | 销量 / 销售额 / 曝光UV |

样例（最新一天 Top5，2019-11-03）：
```
rank=1  product=1005115  apple     sales=491   amount=456,729.25  uv=7,961
rank=2  product=1005105  apple     sales=242   amount=325,869.23  uv=4,917
rank=3  product=1004249  apple     sales=262   amount=189,782.80  uv=4,542
rank=4  product=1005135  apple     sales=112   amount=185,575.10  uv=2,278
rank=5  product=1004767  samsung   sales=734   amount=177,547.46  uv=7,577
```
> 观察：热榜被 **apple** 主导（全期稳定），个别高销量商品（samsung 1004767）靠量上榜。

### 2.4 `ads_session_behavior_daily` —— 会话行为（源 `dws_user_session_daily`）
行数 **34**（每天 1 行）

| 列 | 类型 | 口径 |
|---|---|---|
| `event_date` | date | 分区键 |
| `session_cnt` | bigint | 会话数 = `COUNT(*)` |
| `avg_duration_sec` | double | 平均会话时长（秒） |
| `avg_action_cnt` | double | 平均行为数 = `AVG(view+cart+purchase+remove)` |
| `cart_purchase_rate` | double | 加购转化率 = `SUM(purchase_cnt)/SUM(cart_cnt)` |

样例：
```
2019-10-01  session=268,481  avg_duration=349.05s  avg_action=4.62  cart→purchase=119.2%
2019-10-03  session=240,926  avg_duration=359.07s  avg_action=4.67  cart→purchase=103.0%
```

### 2.5 `ads_user_rfm_snapshot` —— 用户 RFM 分群（源 `dwd_event_fact` purchase 明细，全量快照）
行数 **373,611**（全期购买用户）

| 列 | 类型 | 口径 |
|---|---|---|
| `user_id` | bigint | 用户ID（主键） |
| `r_days` | bigint | Recency = `DATEDIFF(MAX(event_date), MAX(purchase日期))` |
| `f_cnt` | bigint | Frequency = 累计购买次数 |
| `m_amount` | double | Monetary = 累计购买金额 |
| `rfm_seg` | varchar(32) | 8 分群：`AVG(f_cnt)/AVG(m_amount)` 阈值 + R≤30/R>30 两档 |

**分群分布（用户数 / GMV）**：

| 分群 | 用户数 | 占比 | GMV |
|---|---|---|---|
| 一般价值客户 | 243,265 | 65.1% | 47,784,401 |
| **重要价值客户** | 45,315 | 12.1% | **138,958,136（占全期 GMV 55.6%）** |
| 重要发展客户 | 38,291 | 10.2% | 45,936,660 |
| 重要保持客户 | 25,532 | 6.8% | 9,169,269 |
| 一般挽留客户 | 17,275 | 4.6% | 3,339,845 |
| 重要唤回客户 | 2,680 | 0.7% | 3,077,941 |
| 重要挽留客户 | 717 | 0.2% | 1,471,849 |
| 一般保持客户 | 536 | 0.1% | 191,013 |

> **业务洞察**：12.1% 的「重要价值客户」贡献 55.6% 的 GMV —— 二八法则显著，运营重点应聚焦该群体。

样例：
```
user_id=340041246  r=20  f=4  m=915.52  重要价值客户
user_id=384989212  r=2   f=2  m=82.60   一般价值客户
```

---

## 三、血缘与对账关系

```
dwd_event_fact (46.9M)
 ├─┬→ dws_user_session_daily (10,278,076)  ─→ ads_session_behavior_daily (34)
 │ └→ ads_user_rfm_snapshot (373,611)          ← 回 DWD 精确去重
 ├─→ dws_product_daily (2,521,056) ─────────→ ads_product_hot_rank_daily (3,400)
 └─→ dws_user_behavior_daily (7,169,053) ───→ ads_trade_daily (34)
                           └─(漏斗 UV 需跨商品去重，故 ads_conversion_funnel_daily 回 DWD)──→ (34)
```

对账不变量：
1. **总量守恒**：3 张 DWS 的 purchase 计数 == 源 **809,238**
2. **交叉守恒**：`dws_product_daily.sales_amount` == `dws_user_behavior_daily.purchase_amount` == `ads_trade_daily.gmv` == **249,929,113.38**
3. **漏斗不变量**：逐日 `view_uv >= cart_uv 且 view_uv >= purchase_uv`（0 违规）
4. **行数校验**：热榜 34×100=3,400；trade/funnel/session 各 34；RFM == 购买用户 373,611

---

## 四、口径说明

- **UV** 一律 `COUNT(DISTINCT user_id)`；漏斗跨商品去重，避免商品级 UV 累加重复计。
- **销量/销售额**只统计 `purchase` 行为。
- 两引擎（Spark / StarRocks）同结构同口径，行数与关键指标完全一致，可互相对账。
- Spark 侧列类型为 parquet 对应类型（`varchar→string`、`double→double`、`bigint→bigint`）。
