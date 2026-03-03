FROM python:3.11-slim

RUN groupadd -r dbt && useradd -r -g dbt dbt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

USER dbt
WORKDIR /app

ENTRYPOINT ["dbt"]
