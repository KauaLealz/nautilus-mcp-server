FROM python:3.12-slim
RUN pip install --no-cache-dir pymssql
WORKDIR /init
COPY run_init.py .
ENTRYPOINT ["python", "run_init.py"]
