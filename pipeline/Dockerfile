# Always start with FROM
FROM python:3.13.11-slim
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
# dependencies are exactly the same as the python env
RUN uv sync --locked 

# Specify your directory
COPY pipeline.py .

# Default command to run
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]