from setuptools import find_packages, setup


setup(
    name="queue-bot",
    version="0.1.0",
    description="Telegram-бот для управления учебной очередью, расписанием и сдачей заданий",
    author="Timofey",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "queue_bot": ["index.html"],
    },

    # Основные зависимости для работы бота
    install_requires=[
        "aiogram>=3.20.0,<3.27.0",
        "aiosqlite==0.20.0",
        "aiohttp>=3.11.0,<3.13.0",
        "python-dotenv==1.0.1",
    ],

    # Дополнительные зависимости для разработки и сборки документации
    extras_require={
        "docs": [
            "mkdocs>=1.5.0",
            "mkdocs-material>=9.0.0",
            "mkdocstrings[python]>=0.24.0",
            "griffe>=0.38.0",
        ],
        "dev": [
            "build>=1.2.2",
            "setuptools>=69.0.0",
            "wheel>=0.43.0",
            "mkdocs>=1.5.0",
            "mkdocs-material>=9.0.0",
            "mkdocstrings[python]>=0.24.0",
            "griffe>=0.38.0",
        ],
    },

    entry_points={
        "console_scripts": [
            "queue-bot=queue_bot.main:run",
        ],
    },
    python_requires=">=3.11",
)
