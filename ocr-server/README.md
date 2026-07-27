# guguji OCR / QDII 服务

## 本地启动

```bash
pip install -r requirements.txt
python app.py
```

## QDII 额度 API

- `GET /api/qdii/health`
- `GET /api/qdii/batch?codes=016664,539002`
- `GET /api/qdii/<code>`
- `GET /api/qdii/<code>/history`

## QDII 雷达扫描（AI 算力 / 半导体产业链）

后台扫描模块（**暂无推送**）：

| 文件 | 说明 |
|---|---|
| `qdii_radar.py` | 候选池、相似度、精池、限额事件 |
| `qdii_radar_config.json` | 可覆盖默认 seed / 阈值等 |
| `scan_qdii_radar.py` | 定时任务 CLI |

### 监控标的

- **硬件核心 weight=1.0**：台积电、阿斯麦、AMD、英特尔、ARM、三星、美光、SK海力士、英伟达、Marvell、Tower、GlobalFoundries、Lumentum、Coherent
- **CSP weight=0.85（略低）**：谷歌、微软、Meta、苹果、亚马逊（不加特斯拉）

### CLI

```bash
# 全量：主题池打分 + 精池限额扫描
python scan_qdii_radar.py full

# 仅日更候选池/打分
python scan_qdii_radar.py universe

# 仅精池限额
python scan_qdii_radar.py quota

python scan_qdii_radar.py status
python scan_qdii_radar.py pool
python scan_qdii_radar.py events
```

建议 cron（Asia/Shanghai）：

```text
# 交易时段每 15 分钟扫限额
*/15 9-15 * * 1-5 cd /app && python scan_qdii_radar.py quota

# 每天 08:30 / 18:30 刷新主题池与持仓相似度
30 8,18 * * 1-5 cd /app && python scan_qdii_radar.py universe
```

### Radar API

- `GET /api/qdii/radar/status`
- `GET /api/qdii/radar/pool`
- `GET /api/qdii/radar/events?days=7`
- `GET /api/qdii/radar/scores`
- `POST /api/qdii/radar/run?mode=full|universe|quota`  
  - 若设置环境变量 `QDII_RADAR_TOKEN`，需带 `X-Radar-Token` 或 `?token=`

事件类型：

- `E1_quota_loosen` 放额
- `E2_new_pool` 上新入池（冷启动不写）
- `E3_quota_tighten` 从紧

数据表与额度快照共用 `data/qdii.db`（可用 `QDII_DB_PATH` 覆盖）。
