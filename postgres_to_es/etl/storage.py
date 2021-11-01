import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, "w") as file:
            file.truncate()
            file.write(json.dumps(state))

    def retrieve_state(self) -> dict:
        with open(self.file_path, "r") as file:
            state = json.loads(file.read())
        return state


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        state = {key: value}
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        state = self.storage.retrieve_state()
        return state.get(key, None)
