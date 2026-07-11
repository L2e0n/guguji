# OCR Server

本目录是 `guguji` 的 OCR 后端原型，供 `api.guguji.icu/ocr` 反代调用。

## 本地运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

服务端代码已经兼容较新的 PaddleOCR 3.x，并在 CPU 环境下默认关闭 `mkldnn`、禁用 PIR API，避免部分主机上的推理异常。

## Docker 运行

```bash
docker build -t guguji-ocr .
docker run -d --name guguji-ocr -p 5000:5000 --restart unless-stopped guguji-ocr
```

## Cloudflare Worker 对接

Worker 需要配置：

```bash
npx wrangler secret put OCR_API_BASE
```

值填 OCR 服务的公网基址，例如：

```text
http://<your-server-ip>:5000
```

然后重新部署 `guguji-proxy`。
