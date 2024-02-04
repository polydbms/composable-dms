FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive
ENV USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libc-dev \
        make \
        cmake \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/eyalroz/tpch-dbgen.git

RUN set -x \
    && cd tpch-dbgen \
    && cmake -D CSV_OUTPUT_FORMAT:BOOL=OFF -D EOL_HANDLING:BOOL=ON . \
    && make

COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]