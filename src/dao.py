#!/usr/bin/env python3

import psycopg2
from sql_factory import SQL_FACTORY
from iterators import CursorIter, ItemIter


class DAO:

    _conn = None
    _conn_user_cnt = 0

    def __init__(self, db_config, attrs):
        if self.__class__._conn is None or self.__class__._conn_user_cnt == 0:
            self.__class__._conn = psycopg2.connect(**db_config)
        self.__class__._conn_user_cnt += 1
        self._query_cur = self._conn.cursor()
        self._iter_cur = self._conn.cursor()
        self._attrs = attrs
        self._length = None
        self._start_iteration = False

    def __del__(self):
        try:
            self.__class__._conn_user_cnt -= 1
            if self.__class__._conn_user_cnt == 0: self.__class__._conn.close()
        except AttributeError:
            pass

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

    def __iter__(self):
        self._iter_cur.execute(SQL_FACTORY["iter_node"])
        return CursorIter(self._iter_cur)


class NodeDAO(DAO):
    def __getitem__(self, n):
        self._query_cur.execute(
            SQL_FACTORY["query_node"].replace("<attrs>", ', '.join(self._attrs)), (n, )
        )
        node_attr = self._query_cur.fetchall()
        if not node_attr:
            raise KeyError(n)
        return dict(zip(self._attrs, node_attr[0]))

    def items(self):
        self._iter_cur.execute(SQL_FACTORY["iter_node"])
        return list(ItemIter(self))


class AdjDAO(DAO):
    def __getitem__(self, n):
        if n not in self:
            raise KeyError(n)
        self._query_cur.execute(
            SQL_FACTORY["query_adj"].replace("<attrs>", ', '.join(self._attrs)), (n, n)
        )
        edge_attr = self._query_cur.fetchall()
        if not edge_attr:
            return {}
        return {rec[0]: dict(zip(self._attrs, rec[1:])) for rec in edge_attr}

    def iter_items(self):
        self._iter_cur.execute(SQL_FACTORY["iter_node"])
        return ItemIter(self)

    def items(self):
        return list(self.iter_items())


class SuccDao(DAO):
    def __getitem__(self, n):
        if n not in self:
            raise KeyError(n)
        self._query_cur.execute(
            SQL_FACTORY["query_succ"].replace("<attrs>", ', '.join(self._attrs)), (n, )
        )
        edge_attr = self._query_cur.fetchall()
        if not edge_attr:
            return {}
        return {rec[0]: dict(zip(self._attrs, rec[1:])) for rec in edge_attr}

    def iter_items(self):
        self._iter_cur.execute(SQL_FACTORY["iter_node"])
        return ItemIter(self)

    def items(self):
        return list(self.iter_items())


class PredDao(SuccDao):

    def __getitem__(self, n):
        if n not in self:
            raise KeyError(n)
        self._query_cur.execute(
            SQL_FACTORY["query_pred"].replace("<attrs>", ', '.join(self._attrs)), (n, )
        )
        edge_attr = self._query_cur.fetchall()
        if not edge_attr:
            return {}
        return {rec[0]: dict(zip(self._attrs, rec[1:])) for rec in edge_attr}
