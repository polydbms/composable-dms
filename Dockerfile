FROM continuumio/miniconda3

COPY environment.yml .

RUN conda env create -f environment.yml

SHELL ["conda", "run", "-n", "composable-dms", "/bin/bash", "-c"]

ADD requirements.txt /

RUN pip install -r requirements.txt

RUN echo "Run Test Docker"

COPY main.py .
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "composable-dms", "python", "main.py"]
