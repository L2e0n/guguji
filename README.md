# guguji
咕咕鸡大作战-基金估值工具 v5.4.0

非程序员，vibe coding 纯手搓玩具

## 功能
- 📊 **多组合管理**：多个基金组合分组管理
- 🔄 **四源估值**：天天基金(估值) + 雪球(实时) + 新浪(官方) + 东方财富(行情)
- 🎨 **涨跌颜色**：红色涨↑ 绿色跌↓ 灰色持平
- 📋 **Excel导入**：批量导入持仓
- 📷 **OCR识别**：截图自动识别持仓（需运行 OCR 后端）
- 💾 **本地存储**：数据全在浏览器 localStorage

## 数据源
| 来源 | 接口 | 说明 |
|------|------|------|
| 天天基金 | fundgz.1234567.com.cn | 盘中估值(JSONP) |
| 雪球 | api.guguji.icu/xq/ | 实时行情(需proxy) |
| 新浪 | api.guguji.icu/sina/ | 官方净值(需proxy) |
| 东方财富 | push2.eastmoney.com | LOF/ETF实时行情(JSONP) |

## OCR 服务（可选）
运行 Python OCR 后端可支持截图识别持仓：
```bash
cd ocr-server
pip install -r requirements.txt
python app.py
```
然后访问 guguji.icu，点击「📷 OCR 识别」上传持仓截图。

## 已知问题
- LOF 基金和 024773 这类天天估值不支持的，使用东方财富行情做补充
- OCR 识别需要手机持仓截图，测试阶段建议先用 Excel 导入

## 联系
leon.zhh@foxmail.com
