FROM python:3.12-slim

WORKDIR /app

# osmium-tool trims the California extract before Python turns it into the
# small, editable Lake-region graph.
RUN apt-get update \
    && apt-get install -y --no-install-recommends osmium-tool \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY mcp_server ./mcp_server

EXPOSE 8123
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8123"]
