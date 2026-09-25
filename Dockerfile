FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CALIBRATION_DB=/data/calibration.db

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY templates ./templates
COPY static ./static
COPY tests ./tests
COPY scripts ./scripts
COPY pytest.ini ./

RUN mkdir -p /data && chmod +x scripts/verify.py
VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 \
    CMD python -c "import json,urllib.request; assert json.load(urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3))['status']=='ok'"

CMD ["gunicorn", "--workers=4", "--bind=0.0.0.0:8080", "--timeout=30", "app.server:create_app()"]
