# Deployment

## Local start

```bash
python -m queue_bot
```

## Docker

### Build image

```bash
docker build -t queue-bot .
```

### Prepare env file

Create a local `.env` file from `.env.example` and set:

- `BOT_TOKEN` - Telegram bot token.
- `ADMIN_IDS` - comma-separated Telegram user IDs of admins.
- `PORT=3000`
- `WEB_URL=https://your-domain.example`
- `DB_PATH=/data/queue.db`

### Run container

```bash
docker run -d \
  --name queue-bot \
  --restart unless-stopped \
  --env-file .env \
  -p 3000:3000 \
  -v queue-bot-data:/data \
  queue-bot
```

The named volume stores the SQLite database and keeps bot data after container recreation.

### Reverse proxy

For production, expose the bot through Nginx or Caddy and point `WEB_URL` to the public HTTPS URL that Telegram users will open inside the WebApp.

## Package build

```bash
python -m pip install -r requirements-dev.txt
python -m build
```

## Documentation build

```bash
python -m mkdocs build --strict
```

## Railway and Procfile

Project launch commands are aligned with package execution and use:

```bash
python -m queue_bot
```

This avoids dependence on a root-level `main.py` file and matches the `setuptools` console entry point.
