FROM python:3.11-slim

RUN groupadd -r dbt && useradd -r -g dbt -m dbt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /app && chown dbt:dbt /app
USER dbt
WORKDIR /app

ENTRYPOINT ["dbt"]
