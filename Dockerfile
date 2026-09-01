FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg curl ca-certificates locales \
    xvfb x11vnc supervisor fontconfig \
    fonts-liberation fonts-dejavu-core fonts-roboto \
    python3 python3-pip python3-venv \
    git build-essential unzip \
  && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update && apt-get install -y google-chrome-stable \
  && rm -rf /var/lib/apt/lists/*

# venv for pip installs (Debian bookworm blocks system-wide pip installs)
RUN python3 -m venv /data/venv
ENV PATH="/data/venv/bin:${PATH}"

RUN mkdir -p /data/chrome-profile /data/outputs /data/admin /data/snapshots /data/workspace

WORKDIR /app/mcp-server
COPY mcp-server/package.json .
RUN npm install --omit=dev
COPY mcp-server/server.js .

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 3001 5900 9222

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
