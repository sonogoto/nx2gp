#!/usr/bin/env python3


class CursorIter:
    def __init__(self, cur):
        self._cur = cur

    def __next__(self):
        node = self._cur.fetchone()
        if node:
            return node[0]
        else:
            raise StopIteration

    def __iter__(self):
        return self


class ItemIter:
    def __init__(self, adj_dao):
        self._adj_dao = adj_dao
        self._adj_iter = iter(adj_dao)

    def __next__(self):
        adj_node = next(self._adj_iter)
        return adj_node, self._adj_dao[adj_node]

    def __iter__(self):
        return self

