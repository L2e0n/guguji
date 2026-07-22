# guguji
咕咕鸡大作战-基金估值工具 

非程序员，vibe coding 纯手搓玩具
v5.5.4
## 功能
- 📊 **多组合管理**：多个基金组合分组管理
- 🔄 **多源估值**：东财fundgz(主) + 天天fundgz(兜底) + 东方财富行情 + 新浪官方净值 
- 🎨 **涨跌颜色**：红色涨↑ 绿色跌↓ 灰色持平
- 📋 **Excel导入**：批量导入持仓
- 💾 **本地存储**：数据全在浏览器 localStorage
- 📷 **OCR识别**：截图自动识别持仓，支持导入前手工确认（需配置 `api.guguji.icu/ocr` 后端）

## 部署说明
- 前端静态页：`index.html`（GitHub Pages + Cloudflare，域名 `ji.guguji.icu`）
- 行情 / OCR 代理：`api.guguji.icu`（Cloudflare Worker，源码原路径 `../guguji-proxy/worker.js`）
- OCR + 估值代理：`ocr.guguji.icu` → 阿里云 `/opt/guguji-ocr`（`ocr-server/app.py`）
  - `GET /api/fundgz/{code}`：东财估值主源 + 天天估值兜底（服务端带东财 Referer）
- 注意：阿里云目前承载 OCR/估值代理，**不是**前端静态站本体

### OCR 接入
- Worker 通过环境变量 `OCR_API_BASE` 转发 `POST /ocr` 到 `${OCR_API_BASE}/api/ocr`
- 健康检查走 `${OCR_API_BASE}/health`
- 未配置 `OCR_API_BASE` 时，前端会自动显示 `OCR 未配置，先保留 Excel 导入`


## 联系
leon.zhh@foxmail.com
