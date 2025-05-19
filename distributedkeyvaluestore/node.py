from abc import ABC, abstractmethod
from typing import Optional

class Node(ABC):

    @property
    @abstractmethod
    def node_id(self) -> int:
        ...

    @abstractmethod
    def put(self, key: str, value: bytes) -> bool:
        ...

    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        ...

class KVNode(Node):

    def __init__(self, node_id: int):
        self.data: dict[str, bytes] = {}
        self._nid = node_id

    @property
    def node_id(self):
        return self._nid

    def put(self, key: str, value: bytes) -> bool:
        # capacity check maybe?
        self.data[key] = value
        return True

    def get(self, key: str) -> Optional[bytes]:
        return self.data.get(key)

    def delete(self, key: str) -> bool:
        if key not in self.data:
            return False
        del self.data[key]
        return True
