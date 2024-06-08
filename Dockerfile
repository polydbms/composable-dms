FROM continuumio/miniconda3

RUN apt-get update && apt-get install -y sudo
RUN adduser --disabled-password -u 10057 -g 10057 --gecos '' docker
RUN adduser docker sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER docker

COPY environment.yml .
COPY configuration.yml .

RUN conda env create -f environment.yml

SHELL ["conda", "run", "-n", "composable-dms", "/bin/bash", "-c"]

ADD requirements.txt /

RUN pip install -r requirements.txt

RUN echo "Run Docker"

COPY queries/ /queries/
COPY benchmark_results/ /benchmark_results/
COPY substrait_consumer/ /substrait_consumer/
COPY substrait_producer/ /substrait_producer/

COPY main.py .
COPY compo_db.py .
COPY test_result.py .
COPY times.py .

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "composable-dms", "python", "main.py"]
