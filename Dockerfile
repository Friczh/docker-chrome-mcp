FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg curl ca-certificates \
    xvfb x11vnc supervisor fontconfig \
    fonts-liberation fonts-dejavu-core fonts-roboto \
    nodejs npm \
  && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update && apt-get install -y google-chrome-stable \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data/chrome-profile

WORKDIR /app/mcp-server
COPY mcp-server/package.json .
RUN npm install --omit=dev
COPY mcp-server/server.js .

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 3001 5900 9222

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
