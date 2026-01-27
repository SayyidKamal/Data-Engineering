# FROM python:3.13.11-slim
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# #RUN pip install pandas pyarrow

# #WORKDIR /code
# #COPY pipeline.py .
# #ENTRYPOINT [ "python" , "pipeline.py" ]

# # WORKDIR /code
# # instead of using uv run, we can just add .venv/bin to PATH
# ENV PATH="/workspaces/Data-Engineering/pipeline/.venv/bin:${PATH}"
# COPY pyproject.toml .python-version uv.lock ./
# RUN uv sync --locked

# COPY ingest_data.py .
# #COPY pipeline.py .

# #ENTRYPOINT [ "uv", "run", "python" , "pipeline.py" ]
# #ENTRYPOINT ["python" , "pipeline.py" ]
# #ENTRYPOINT ["python" , "ingest_data.py" ]
# ENTRYPOINT ["bash"]



FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]
