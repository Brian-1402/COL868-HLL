FROM postgres:17

# Install dependencies and build HLL
RUN apt-get update && \
    apt-get install -y git build-essential postgresql-server-dev-17 && \
    git clone https://github.com/citusdata/postgresql-hll.git /tmp/hll && \
    cd /tmp/hll && make && make install && \
    rm -rf /var/lib/apt/lists/* /tmp/hll
