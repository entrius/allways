import json
import logging
import os
from pathlib import Path


class Ledger:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            with self.path.open() as file:
                return json.load(file)
        except (json.JSONDecodeError, OSError) as error:
            logging.getLogger('ledger').warning('Ledger unreadable path=%s error=%s', self.path, error)
            return {}

    def save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(f'{self.path.suffix}.tmp')
        with temporary.open('w') as file:
            json.dump(state, file, default=str)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.path)
