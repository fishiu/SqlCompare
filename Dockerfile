# Dockerfile
FROM python:3.9-slim

# Set environment variables to non-interactive to avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package index and install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        gnupg \
        lsb-release \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install DuckDB
RUN pip install duckdb

# Expose no specific port for this container as it will be used for Python scripts
CMD ["tail", "-f", "/dev/null"]
