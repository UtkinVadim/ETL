import abc
import json
from pathlib import Path
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            with open(self.file_path, 'w') as file:
                file.write('{}')

    def save_state(self, state: dict) -> None:
        with open(self.file_path, "w") as file:
            file.write(json.dumps(state))

    def retrieve_state(self) -> dict:
        with open(self.file_path, "r") as file:
            state = json.loads(file.read())
        return state


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        state = self.storage.retrieve_state()
        state.update({key: value})
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        state = self.storage.retrieve_state()
        return state.get(key, None)
