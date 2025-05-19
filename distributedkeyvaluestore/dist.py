from distributedkeyvaluestore.node import Node
from typing import Optional
from math import ceil


class DistributedKeyValueStore():
    def __init__(self, nodes: list[Node], replication_factor: int):
        self.nodes: list[Node] = nodes
        self.replication_factor = min(len(self.nodes), replication_factor)

    def _get_primary_node_for_key(self, key: str) -> Node:
        # consistent hashing could be used here
        index = abs(hash(key) % len(self.nodes))
        return self.nodes[index]

    def _get_replica_nodes(self, primary: Node) -> list[Node]:
        replicas: list[Node] = []
        if len(self.nodes) == 1 or self.replication_factor <= 1:
            return replicas

        primary_index = self.nodes.index(primary)
        for i in range(self.replication_factor):
            replica_index = (primary_index + i) % len(self.nodes)
            if not primary_index == replica_index:
                replicas.append(self.nodes[replica_index])

            if len(replicas) == self.replication_factor - 1:
                break

        return list(set(replicas))

    def get(self, key: str) -> Optional[bytes]:
        primary = self._get_primary_node_for_key(key)

        value = primary.get(key)

        return value

    def put(self, key: str, value: bytes) -> bool:
        primary = self._get_primary_node_for_key(key)

        primary_success = primary.put(key, value)
        if not primary_success:
            return False

        replicas = self._get_replica_nodes(primary)

        replica_successes = 0
        for replica in replicas:
            replica_success = replica.put(key, value)
            if not replica_success:
                # hinted handoff here
                ...
            else:
                replica_successes += 1

        writeQuorum = 1 + ceil((self.replication_factor - 1) / 2)
        if self.replication_factor == 1:
            writeQuorum = 1

        overallSuccess = 1 + replica_successes >= writeQuorum

        return overallSuccess
