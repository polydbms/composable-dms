FROM continuumio/miniconda3

COPY environment.yml .
COPY configuration.yml .

RUN conda env create -f environment.yml

SHELL ["conda", "run", "-n", "composable-dms", "/bin/bash", "-c"]

ADD requirements.txt /

RUN pip install -r requirements.txt

RUN echo "Run Docker"

COPY queries/ /queries/
COPY substrait_consumer/ /substrait_consumer/
COPY substrait_producer/ /substrait_producer/

COPY main.py .
COPY compo_db.py .
COPY test_result.py .
COPY times.py .

RUN addgroup --g 1024 groupcontainer
RUN adduser -u 1024 -G groupcontainer -h /home/containeruser -D containeruser

USER containeruser

RUN mkdir /home/containeruser/benchmark_results

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "composable-dms", "python", "main.py"]
