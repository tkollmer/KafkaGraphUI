# Stage 1: Build React frontend
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

# Stage 2: Python runtime
FROM python:3.12-slim AS runtime
WORKDIR /app

# Non-root user
RUN useradd -m -u 1000 appuser

# Install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend
COPY backend/ .

# Copy frontend build
COPY --from=frontend-build /app/frontend/dist ./static/

# Switch to non-root
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/health')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
