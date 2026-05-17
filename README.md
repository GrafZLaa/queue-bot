# Queue Bot

`Queue Bot` - Telegram-бот и Telegram WebApp для управления очередями сдачи практических работ. Студент видит расписание своей группы, задания и очередь на сдачу. Преподаватель создаёт предметы и пары, открывает запись, формирует порядок сдающих и отмечает результат.

## Возможности

- регистрация студента через `/start` с ФИО и учебной группой;
- Telegram WebApp в стиле журнала занятий: календарь месяца, карточки пар, задания и очередь;
- фильтрация расписания по группе студента;
- запись и выход из очереди только для подтверждённого Telegram WebApp пользователя;
- проверка Telegram `initData` на сервере;
- админский режим в боте и WebApp;
- длительность пары хранится явно, поэтому время окончания считается корректно;
- рейтинг студента и сортировка очереди по категориям;
- перенос несдавших на следующую пару;
- SQLite-хранилище, Dockerfile, MkDocs-документация и тесты.

## Структура проекта

- `queue_bot/main.py` - Telegram handlers, HTTP API, WebApp auth and admin routes.
- `queue_bot/database.py` - SQLite schema, migrations and queue logic.
- `queue_bot/webapp_auth.py` - validation of Telegram WebApp `initData`.
- `queue_bot/index.html` - Telegram WebApp interface.
- `docs/` - project documentation for MkDocs.
- `tests/` - unit tests for database logic and WebApp auth.

## Переменные окружения

```env
BOT_TOKEN=123456:telegram-token
ADMIN_IDS=123456789,987654321
PORT=3000
WEB_URL=https://your-public-url.example
DB_PATH=/data/queue.db
```

Для локальной проверки WebApp API вне Telegram можно временно включить:

```env
ALLOW_UNVERIFIED_WEBAPP=1
```

В продакшене эту переменную включать не нужно: сервер должен принимать действия записи только с валидным Telegram `initData`.

## Локальный запуск

```bash
python -m pip install -r requirements.txt
python -m queue_bot
```

После установки пакета доступна команда:

```bash
queue-bot
```

## Docker

```bash
docker build -t queue-bot .
docker run --env-file .env -p 3000:3000 -v queue-bot-data:/data queue-bot
```

## Тесты

```bash
python -m unittest discover -s tests
```

## Сборка пакета и документации

```bash
python -m pip install -r requirements-dev.txt
python -m build
python -m mkdocs build --strict
```

## Сценарий демонстрации

1. Студент пишет `/start`, вводит ФИО и группу.
2. Открывает WebApp из кнопки “Открыть журнал занятий”.
3. Выбирает дату в календаре и открывает карточку пары.
4. Записывается в открытую очередь.
5. Администратор закрывает очередь, порядок формируется автоматически.
6. Преподаватель отмечает результат сдачи, рейтинг студента обновляется.
