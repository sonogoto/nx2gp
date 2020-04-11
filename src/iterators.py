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
    def __init__(self, dao):
        self._dao = dao
        self._iter = iter(dao)

    def __next__(self):
        node = next(self._iter)
        return node, self._dao[node]

    def __iter__(self):
        return self

