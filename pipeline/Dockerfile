FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN pip install pandas pyarrow

#WORKDIR /code
#COPY pipeline.py .
#ENTRYPOINT [ "python" , "pipeline.py" ]

# WORKDIR /code
# instead of using uv run, we can just add .venv/bin to PATH
ENV PATH="/code/.venv/bin:${PATH}"
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked
COPY pipeline.py .
#ENTRYPOINT [ "uv", "run", "python" , "pipeline.py" ]
ENTRYPOINT ["python" , "pipeline.py" ]

