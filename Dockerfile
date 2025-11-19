# Dockerfile for replay-cdxj-indexing-tools
# Build and run CDXJ indexing tools in a containerized environment

FROM python:3.11-slim

LABEL maintainer="arquivo@fccn.pt"
LABEL description="Tools for web archive replay CDXJ indexing"
LABEL version="1.0.0"

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    parallel \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml ./

# Install Python package
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy the rest of the files
COPY README.md LICENSE ./
COPY replay_cdxj_indexing_tools/ ./replay_cdxj_indexing_tools/

# Create directories for input/output
RUN mkdir -p /data/input /data/output

# Set volume mount points
VOLUME ["/data/input", "/data/output"]

# Set working directory to /data for easy file access
WORKDIR /data

# Default command shows available tools
CMD ["sh", "-c", "echo 'Available commands:' && \
     echo '  merge-cdxj' && \
     echo '  cdxj-to-zipnum' && \
     echo '  filter-excessive-urls' && \
     echo '  filter-blocklist' && \
     echo '  cdxj-index-collection' && \
     echo '' && \
     echo 'Example usage:' && \
     echo '  docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools merge-cdxj --help' && \
     echo '  docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools cdxj-to-zipnum -i input/file.cdxj -o output/'"]
