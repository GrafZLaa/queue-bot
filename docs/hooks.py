import sys
import os

def on_config(config, **kwargs):
    """Добавляет корень проекта в sys.path."""
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if root not in sys.path:
        sys.path.insert(0, root)
    return config