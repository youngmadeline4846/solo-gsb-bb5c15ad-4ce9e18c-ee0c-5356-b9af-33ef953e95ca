FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 先装依赖，充分利用镜像层缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# SQLite 数据目录（Compose 中挂载卷持久化）；建表由应用启动时幂等完成
RUN mkdir -p /data
ENV DATABASE_PATH=/data/alerts.db \
    SILENCE_INTERVAL_SECONDS=300

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
