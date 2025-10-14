# Official Python runtime
FROM python:3.13-slim-bullseye

# Set workdir
WORKDIR /app

# Prevents Python from writing pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
# Prevents Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED 1

# Install system dependencies (if needed) and curl for health checks
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        unixodbc \
        unixodbc-dev \
        libodbc1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
# Copy the full project into the image. Build context should be the repository root
# so this will include the `backend/` folder alongside `app_local/`.
COPY . /app

# Expose port
EXPOSE 8000

# Flask env
ENV FLASK_APP=app_local
ENV FLASK_RUN_PORT=8000
ENV FLASK_RUN_HOST=0.0.0.0

# Default command to run the Flask app (production suggestion: use gunicorn externally)
CMD ["flask", "run"]
