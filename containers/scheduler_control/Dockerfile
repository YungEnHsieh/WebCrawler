FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    sqlalchemy==2.0.36 \
    psycopg2-binary==2.9.9 \
    pyyaml==6.0.2 \
    python-json-logger==2.0.7 \
    joblib==1.5.3 \
    numpy==2.1.2 \
    scipy==1.17.0 \
    scikit-learn==1.8.0

COPY containers/scheduler_control/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
