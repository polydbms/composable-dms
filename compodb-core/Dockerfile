FROM continuumio/miniconda3 AS base

WORKDIR /app

# Copy and install Conda environment + dependencies
COPY environment.yml /app
COPY requirements.txt /app

RUN conda env create -f environment.yml && conda clean --all -y
RUN conda run -n composable-dms pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl graphviz && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    /root/.cargo/bin/rustc --version && \
    rm -rf /root/.cargo/registry /root/.cargo/git

WORKDIR /app/connectors/http
# FastAPI port
EXPOSE 8000

# Start FastAPI server
CMD ["conda", "run", "--no-capture-output", "-n", "composable-dms", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]