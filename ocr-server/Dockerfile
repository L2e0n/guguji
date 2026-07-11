FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python - <<'PY' || exit 1
import urllib.request
urllib.request.urlopen("http://127.0.0.1:5000/health", timeout=3).read()
PY

CMD ["gunicorn", "-w", "1", "-k", "gthread", "--threads", "4", "-b", "0.0.0.0:5000", "app:app"]
