FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml requirements.lock README.md ./
COPY src ./src
COPY examples ./examples
RUN pip install --no-cache-dir -r requirements.lock . && useradd --create-home worker \
    && mkdir /data && chown worker:worker /data
USER worker
CMD ["python", "-m", "workflow_orchestrator.worker"]
