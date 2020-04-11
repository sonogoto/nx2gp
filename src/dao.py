#!/usr/bin/env python3

import psycopg2
from sql_factory import SQL_FACTORY
from iterators import CursorIter, ItemIter


class DAO:
    def __init__(self, db_config, attrs):
        self._conn = psycopg2.connect(**db_config)
        self._query_cur = self._conn.cursor()
        self._iter_cur = self._conn.cursor()
        self._attrs = attrs
        self._length = None
        self._start_iteration = False

    def __del__(self):
        self._conn.close()


class NodeDAO(DAO):
    def __getitem__(self, n):
        self._query_cur.execute(
            SQL_FACTORY["query_node"].replace("<attrs>", ', '.join(self._attrs)), (n, )
        )
        node_attr = self._query_cur.fetchall()
        if not node_attr:
            raise KeyError(n)
        return dict(zip(self._attrs, node_attr[0]))

    def __iter__(self):
        self._iter_cur.execute(SQL_FACTORY["iter_node"])
        return CursorIter(self._iter_cur)

    def __contains__(self, n):
        if isinstance(n, str) or isinstance(n, int):
            self._query_cur.execute(
                SQL_FACTORY["check_node_exists"], (n, )
            )
            return self._query_cur.fetchall()[0][0] >= 1
        return False

    def __len__(self):
        if not self._length:
            self._query_cur.execute(SQL_FACTORY["count_node"])
            self._length = self._query_cur.fetchall()[0][0]
        return self._length


class AdjDAO(DAO):
    def __getitem__(self, n):
        self._query_cur.execute(
            SQL_FACTORY["query_adj"].replace("<attrs>", ', '.join(self._attrs)), (n, n)
        )
        edge_attr = self._query_cur.fetchall()
        if not edge_attr:
            raise KeyError(n)
        return {rec[0]: dict(zip(self._attrs, rec[1:])) for rec in edge_attr}

    def __iter__(self):
        self._iter_cur.execute(SQL_FACTORY["iter_adj"])
        return CursorIter(self._iter_cur)

    def __contains__(self, n):
        if isinstance(n, str) or isinstance(n, int):
            self._query_cur.execute(SQL_FACTORY["check_adj_exists"], (n, n))
            return self._query_cur.fetchall()[0][0] >= 1
        return False

    def __len__(self):
        if not self._length:
            self._query_cur.execute(SQL_FACTORY["count_adj"])
            self._length = self._query_cur.fetchall()[0][0]
        return self._length

    def iter_items(self):
        self._iter_cur.execute(SQL_FACTORY["iter_adj"])
        return ItemIter(self)

    def items(self):
        return list(self.iter_items())


