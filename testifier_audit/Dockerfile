FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

RUN python -m venv "$VIRTUAL_ENV" \
    && pip install --upgrade pip setuptools wheel

COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs

RUN pip install .

COPY tests ./tests
COPY Makefile .

ENTRYPOINT ["testifier-audit"]
CMD ["--help"]
