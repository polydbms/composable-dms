FROM continuumio/miniconda3

WORKDIR /app

COPY environment.yml /app
RUN conda env create -f environment.yml

SHELL ["conda", "run", "-n", "composable-dms", "/bin/bash", "-c"]

# Install Rust (required for compiling some Python packages)
RUN apt-get update && apt-get install -y curl && \
    curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    echo 'export PATH=$HOME/.cargo/bin:$PATH' >> ~/.bashrc && \
    /bin/bash -c "source ~/.bashrc"

COPY requirements.txt /app

RUN /bin/bash -c "export PATH=$HOME/.cargo/bin:$PATH && pip install -r requirements.txt"

RUN echo "Run Test Docker"
ENV PYTHONPATH=/app

COPY . /app
WORKDIR /app/connectors/http

# FastAPI port
EXPOSE 8000

CMD ["conda", "run", "--no-capture-output", "-n", "composable-dms", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]