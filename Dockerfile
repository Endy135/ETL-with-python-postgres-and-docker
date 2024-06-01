FROM python:3.8-slim

RUN apt-get update && apt-get install -y postgresql-client

COPY script_elt.py .

CMD ["python", "script_elt.py"]