FROM node:22-slim

RUN apt-get update && apt-get install -y \
    proot curl tar procps python3 build-essential \
    skopeo \
    && rm -rf /var/lib/apt/lists/*

# umoci for unpacking OCI images (skopeo fallback path)
RUN curl -Lo /usr/local/bin/umoci \
    https://github.com/opencontainers/umoci/releases/download/v0.4.7/umoci.amd64 \
    && chmod +x /usr/local/bin/umoci

WORKDIR /daemon
COPY package.json .
RUN npm install
COPY index.js .

RUN mkdir -p /servers /image-cache /data

EXPOSE 7860
CMD ["node", "index.js"]
