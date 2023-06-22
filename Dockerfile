FROM amazon/aws-cli:latest
FROM python:3.9-slim

WORKDIR /root/

RUN mkdir -p /root/.aws

COPY ./config /root/.aws/
COPY ./credentials /root/.aws/

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip

RUN git clone https://github.com/924anonymous/MultiUtilityAppIce.git .

RUN pip install -r requirements.txt

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "MainUiApp.py", "--server.port=8501", "--server.address=0.0.0.0"]