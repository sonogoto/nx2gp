#!/usr/bin/env python3
# Author: liusixiang@geetest.com
# Date: 2020/4/2

import networkx as nx
import psycopg2


class Unsupported(Exception):
    pass


def unsupported(err_msg):
    def _unsupported(func):
        def _raise_exception():
            raise Unsupported(err_msg)
        return _raise_exception

    return _unsupported


sql_factory = {
    "count_node": "SELECT COUNT(1) FROM vertices",
    "count_edge": "SELECT COUNT(1) FROM edges",
    "sum_edge_weight": "SELECT SUM(%s) FROM edges",
    "check_node_exists": "SELECT COUNT(1) FROM vertices WHERE id = %s",
    "check_edge_exists": "SELECT COUNT(1) FROM edges WHERE src_id = %s AND dst_id = %s",
    "query_node": "SELECT <attrs> FROM vertices WHERE id = %s",
    "query_edge": "SELECT %s FROM edges WHERE src_id = %s AND dst_id = %s",
    "query_adj": "SELECT edges.dst_id, <attrs> FROM edges WHERE edges.src_id = %s UNION SELECT edges.src_id, <attrs> FROM edges WHERE edges.dst_id = %s",
    "iter_node": "SELECT id FROM vertices",
    "iter_adj": "SELECT DISTINCT src_id FROM edges UNION SELECT DISTINCT dst_id FROM edges",
}


class DAO:
    def __init__(self, db_config, attrs):
        self._conn = psycopg2.connect(**db_config)
        self._query_cur = self._conn.cursor()
        self._iter_cur = self._conn.cursor()
        self._attrs = attrs
        self._cached = []
        self._start_iteration = False

    def __del__(self):
        self._conn.close()


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


class NodeDAO(DAO):
    def __getitem__(self, n):
        self._query_cur.execute(
            sql_factory["query_node"].replace("<attrs>", ', '.join(self._attrs)), (n, )
        )
        node_attr = self._query_cur.fetchall()
        if not node_attr:
            raise KeyError(n)
        return dict(zip(self._attrs, node_attr[0]))

    def __iter__(self):
        self._iter_cur.execute(sql_factory["iter_node"])
        return CursorIter(self._iter_cur)


class AdjDAO(DAO):
    def __getitem__(self, n):
        self._query_cur.execute(
            sql_factory["query_adj"].replace("<attrs>", ', '.join(self._attrs)), (n, n)
        )
        edge_attr = self._query_cur.fetchall()
        if not edge_attr:
            raise KeyError(n)
        return {rec[0]: dict(zip(self._attrs, rec[1:])) for rec in edge_attr}

    def __iter__(self):
        self._iter_cur.execute(sql_factory["iter_adj"])
        return CursorIter(self._iter_cur)

    def iter_items(self):
        self._iter_cur.execute(sql_factory["iter_adj"])
        return ItemIter(self)

    def items(self):
        return list(self.iter_items())


class GraphGP(nx.Graph):

    graph_attr_dict_factory = dict

    def __init__(
            self,
            db_host="127.0.0.1",
            db_port=5432,
            db_user="gpadmin",
            db_passwd=None,
            db_name=None,
            node_attrs=None,
            edge_attrs=None,
            **graph_attr):

        self._db_config = {
            "host": db_host,
            "port": db_port,
            "database": db_name,
            "user": db_user,
            "password": db_passwd
        }
        self._conn = psycopg2.connect(
            **self._db_config
        )
        self._cur = self._conn.cursor()

        self._node_attrs = node_attrs
        self._edge_attrs = edge_attrs
        self.graph = graph_attr
        self._adj = AdjDAO(self._db_config, self._edge_attrs)
        self._node = NodeDAO(self._db_config, self._node_attrs)

        self._nodes_cached = []
        self._edges_cached = []

        self._length = None

    def __del__(self):
        self._conn.close()

    def __iter__(self):
        return iter(self._node)

    def __contains__(self, n):
        if isinstance(n, str) or isinstance(n, int):
            self._cur.execute(sql_factory["check_node_exists"], (n, ))
            return self._cur.fetchall()[0][0] >= 1
        return False

    def __len__(self):
        if not self._length:
            self._cur.execute(sql_factory["count_node"])
            self._length = self._cur.fetchall()[0][0]
        return self._length

    def __getitem__(self, n):
        return self.adj[n]

    @unsupported("Modifying graph is unsupported")
    def add_node(self, node_for_adding, **attr):
        pass

    @unsupported("Modifying graph is unsupported")
    def add_nodes_from(self, nodes_for_adding, **attr):
        pass

    @unsupported("Modifying graph is unsupported")
    def remove_node(self, n):
        pass

    @unsupported("Modifying graph is unsupported")
    def remove_nodes_from(self, nodes):
        pass

    def number_of_nodes(self):
        return self.__len__()

    def order(self):
        return self.__len__()

    def has_node(self, n):
        return self.__contains__(n)

    @unsupported("Modifying graph is unsupported")
    def add_edge(self, u_of_edge, v_of_edge, **attr):
        pass

    @unsupported("Modifying graph is unsupported")
    def add_edges_from(self, ebunch_to_add, **attr):
        pass

    @unsupported("Modifying graph is unsupported")
    def add_weighted_edges_from(self, ebunch_to_add, weight='weight', **attr):
        pass

    @unsupported("Modifying graph is unsupported")
    def remove_edge(self, u, v):
        pass

    @unsupported("Modifying graph is unsupported")
    def remove_edges_from(self, ebunch):
        pass

    @unsupported("Modifying graph is unsupported")
    def update(self, edges=None, nodes=None):
        pass

    def has_edge(self, u, v):
        assert u.__class__.__name__ == v.__class__.__name__
        self._cur.execute(
            sql_factory["check_edge_exists"], (u, v)
        )
        return self._cur.fetchall()[0][0] >= 1

    def get_edge_data(self, u, v, default=None):
        assert u.__class__.__name__ == v.__class__.__name__
        self._cur.execute(
            sql_factory["query_edge"], (', '.join(self._edge_attrs), u, v)
        )
        edge_data = self._cur.fetchone()
        return dict(zip(self._edge_attrs, edge_data[0])) if edge_data else default

    def adjacency(self):
        return self._adj.iter_items()

    @unsupported("Modifying graph is unsupported")
    def clear(self):
        pass

    def copy(self, as_view=False):
        return self.__class__(
            db_host=self._db_config["host"],
            db_port=self._db_config["port"],
            db_user=self._db_config["user"],
            db_passwd=self._db_config["password"],
            db_name=self._db_config["database"],
            node_attrs=self._node_attrs,
            edge_attrs=self._edge_attrs,
            **self.graph
        )

    def to_directed(self, as_view=False):
        pass

    def to_undirected(self, as_view=False):
        return self.copy()

    def size(self, weight=None):
        if weight:
            self._cur.execute(sql_factory["sum_edge_weight"], (weight, ))
            return self._cur.f
            self._cur.execute(sql_factory["count_edge"])
            return self._cur.fetchall()[0][0]

    def number_of_edges(self, u=None, v=None):
        if u is None:
            return int(self.size())
        if v is None:
            return 0
        return int(self.has_edge(u, v))


if __name__ == "__main__":
    G = GraphGP(db_port=15432, db_name="graph", node_attrs=["weight"], edge_attrs=["weight"])
    print(G.adj[35])
    pass
