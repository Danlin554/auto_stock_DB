FROM python:3.12-slim

ENV TZ=Asia/Taipei
ENV PYTHONUNBUFFERED=1

RUN ln -snf /usr/share/zoneinfo/Asia/Taipei /etc/localtime && \
    apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev curl unzip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 安裝 fubon_neo SDK（從官方下載 Linux 版）
RUN curl -L -o /tmp/sdk.zip \
    "https://www.fbs.com.tw/TradeAPI_SDK/fubon_binary/fubon_neo-2.2.8-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.zip" && \
    unzip /tmp/sdk.zip -d /tmp/sdk && \
    pip install --no-cache-dir /tmp/sdk/*.whl && \
    rm -rf /tmp/sdk /tmp/sdk.zip

# 安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/log /app/config && \
    chmod +x /app/entrypoint.sh

EXPOSE 8501

# 由 entrypoint.sh 根據 SERVICE_MODE 決定啟動 dashboard 或 scheduler
ENTRYPOINT ["/app/entrypoint.sh"]
