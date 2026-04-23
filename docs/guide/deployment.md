# Deployment

## Local start

```bash
python -m queue_bot
```

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
