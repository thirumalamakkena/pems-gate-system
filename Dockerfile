FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install dependencies first so this layer is cached
# independently of app code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Default command — used if a service in docker-compose.yml doesn't
# override `command:`. Every consumer overrides this with its own
# `python -m app.consumers.<name>` command.
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000"]