#!/usr/bin/env python3


import networkx as nx
import psycopg2
from unsupported import unsupported
from sql_factory import SQL_FACTORY
from dao import AdjDAO, NodeDAO


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
            self._cur.execute(SQL_FACTORY["check_node_exists"], (n, ))
            return self._cur.fetchall()[0][0] >= 1
        return False

    def __len__(self):
        if not self._length:
            self._cur.execute(SQL_FACTORY["count_node"])
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
            SQL_FACTORY["check_edge_exists"], (u, v)
        )
        return self._cur.fetchall()[0][0] >= 1

    def get_edge_data(self, u, v, default=None):
        assert u.__class__.__name__ == v.__class__.__name__
        self._cur.execute(
            SQL_FACTORY["query_edge"], (', '.join(self._edge_attrs), u, v)
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
            self._cur.execute(SQL_FACTORY["sum_edge_weight"], (weight, ))
        else:
            self._cur.execute(SQL_FACTORY["count_edge"])
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
