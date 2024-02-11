FROM continuumio/miniconda3

COPY environment.yml .
COPY testdb.yml .

RUN conda env create -f environment.yml
RUN conda env create -f testdb.yml

SHELL ["conda", "run", "-n", "composable-dms", "/bin/bash", "-c"]

ADD requirements.txt /

RUN pip install -r requirements.txt

RUN echo "Run Test Docker"

COPY queries/ /queries/

COPY main.py .
COPY tests.py .
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "composable-dms", "python", "main.py"]
