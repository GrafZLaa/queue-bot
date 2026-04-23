# Queue Bot

`Queue Bot` is a Telegram bot for managing academic queues, lesson schedules and assignment tracking. The project combines a Telegram interface for students and administrators with a small web application that shows the current schedule and queue state.

## Project structure

- `queue_bot/main.py` - bot handlers, FSM flows and HTTP API for the web interface.
- `queue_bot/database.py` - asynchronous SQLite layer and application data model.
- `queue_bot/index.html` - static web interface served by `aiohttp`.
- `docs/` - project documentation for MkDocs.

## Dependencies

Runtime dependencies are listed in [requirements.txt](requirements.txt) and duplicated in `pyproject.toml` for package builds:

- `aiogram`
- `aiosqlite`
- `aiohttp`
- `python-dotenv`

Development and documentation dependencies are listed in [requirements-dev.txt](requirements-dev.txt).

## Environment variables

The application uses the following environment variables:

- `BOT_TOKEN` - Telegram bot token.
- `ADMIN_IDS` - comma-separated list of Telegram user IDs with admin access.
- `PORT` - HTTP server port, default is `3000`.
- `WEB_URL` - public URL of the web application shown inside Telegram.

## Installation and launch

1. Install Python `3.11+`.
2. Create and activate a virtual environment.
3. Install runtime dependencies:

```bash
python -m pip install -r requirements.txt
```

4. Copy `.env.example` to `.env` or set the required environment variables in the shell.
5. Start the project:

```bash
python -m queue_bot
```

After installing the package with `pip install .`, you can also run it with:

```bash
queue-bot
```

## Package build

The project uses `setuptools` as the Python build backend. To build source and wheel distributions, install development dependencies and run:

```bash
python -m pip install -r requirements-dev.txt
python -m build
```

The generated artifacts will appear in the `dist/` directory.

## Documentation build

The project documentation is built with MkDocs and `mkdocstrings`:

```bash
python -m pip install -r requirements-dev.txt
python -m mkdocs build --strict
```

The generated static site will appear in the `site/` directory.
