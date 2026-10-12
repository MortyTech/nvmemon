FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# snappy C library for python-snappy
RUN apt-get update \
 && apt-get install -y --no-install-recommends libsnappy1v5 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py nvme.proto profiles.json nvme_pb2.py nvme_pb2_grpc.py ./

ENV NVME_PROFILES_PATH=/app/profiles.json
EXPOSE 8000 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status < 500 else 1)"

# Role is decided purely by env vars:
#   server: NVME_GRPC_LISTEN_PORT>0   agent: NVME_GRPC_SERVER_URL set
CMD ["python", "main.py"]
