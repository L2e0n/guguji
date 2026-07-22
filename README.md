# guguji
咕咕鸡大作战-基金估值工具 

非程序员，vibe coding 纯手搓玩具
v5.5.3
## 功能
- 📊 **多组合管理**：多个基金组合分组管理
- 🔄 **四源估值**：东财fundgz(实时估值) + 东方财富(行情) + 新浪(QDII) 
- 🎨 **涨跌颜色**：红色涨↑ 绿色跌↓ 灰色持平
- 📋 **Excel导入**：批量导入持仓
- 💾 **本地存储**：数据全在浏览器 localStorage
- 📷 **OCR识别**：截图自动识别持仓，支持导入前手工确认（需配置 `api.guguji.icu/ocr` 后端）

## 部署说明
- 前端静态页：`index.html`
- 行情 / OCR 代理：`../guguji-proxy/worker.js`
- 本地 OCR 原型：`ocr-server/app.py`

### OCR 接入
- Worker 通过环境变量 `OCR_API_BASE` 转发 `POST /ocr` 到 `${OCR_API_BASE}/api/ocr`
- 健康检查走 `${OCR_API_BASE}/health`
- 未配置 `OCR_API_BASE` 时，前端会自动显示 `OCR 未配置，先保留 Excel 导入`


## 联系
leon.zhh@foxmail.com
