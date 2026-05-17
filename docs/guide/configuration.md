# Configuration

The bot reads configuration from environment variables and also supports loading values from a local `.env` file.

## Required settings

- `BOT_TOKEN` - Telegram API token for the bot.

## Optional settings

- `ADMIN_IDS` - comma-separated Telegram IDs of administrators.
- `PORT` - HTTP port for the embedded `aiohttp` server. Default: `3000`.
- `WEB_URL` - public URL opened from the Telegram web app button. By default it uses `http://localhost:${PORT}`.
- `DB_PATH` - SQLite database path. In Docker the default path is `/data/queue.db`.
- `ALLOW_UNVERIFIED_WEBAPP` - local development switch for WebApp API calls outside Telegram. Keep it `0` in production.

## Example

```env
BOT_TOKEN=123456:example-token
ADMIN_IDS=123456789,987654321
PORT=3000
WEB_URL=https://example.up.railway.app
DB_PATH=/data/queue.db
ALLOW_UNVERIFIED_WEBAPP=0
```
