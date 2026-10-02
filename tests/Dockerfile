FROM python:3.12-slim
WORKDIR /tests
COPY requirements-test.txt .
RUN pip install --no-cache-dir -r requirements-test.txt
COPY test_health.py .
CMD ["pytest", "-v"]
