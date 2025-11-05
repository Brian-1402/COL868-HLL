# STAGE 1: Builder
# Installs all build-time dependencies, clones, and builds the extension.
FROM postgres:17 AS builder

# Install build dependencies.
# The base postgres:17 image already contains 'clang' for JIT,
# which the 'make' process finds and uses.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-17 \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Clone, build, and install the extension into the builder's PG directories
RUN git clone https://github.com/citusdata/postgresql-hll.git /tmp/postgresql-hll \
    && cd /tmp/postgresql-hll \
    && make \
    && make install


# STAGE 2: Final Image
# Start from a clean Postgres image.
FROM postgres:17

# Copy *only* the required runtime artifacts from the builder stage.

# 1. Copy the compiled shared library
COPY --from=builder /usr/lib/postgresql/17/lib/hll.so /usr/lib/postgresql/17/lib/hll.so

# 2. Copy the extension control and SQL script files
COPY --from=builder /usr/share/postgresql/17/extension/hll* /usr/share/postgresql/17/extension/

# 3. Copy the LLVM bitcode files for JIT
COPY --from=builder /usr/lib/postgresql/17/lib/bitcode /usr/lib/postgresql/17/lib/bitcode

# Add the initialization script to auto-create the extension in new DBs
RUN echo "CREATE EXTENSION IF NOT EXISTS hll;" > /docker-entrypoint-initdb.d/01-create-hll-extension.sql
