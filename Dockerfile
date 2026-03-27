FROM python:3.12-slim

ENV TZ=Asia/Taipei
ENV PYTHONUNBUFFERED=1

RUN ln -snf /usr/share/zoneinfo/Asia/Taipei /etc/localtime && \
    apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/log /app/config

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "dashboard.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.fileWatcherType=none", \
     "--server.headless=true"]
