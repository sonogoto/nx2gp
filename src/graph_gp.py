#!/usr/bin/env python3


import networkx as nx
import psycopg2
from not_permitted import not_permitted
from sql_factory import SQL_FACTORY
from dao import AdjDAO, NodeDAO


class GraphGP(nx.Graph):

    def __init__(
            self,
            db_host="127.0.0.1",
            db_port=15432,
            db_user="gpadmin",
            db_passwd=None,
            db_name="graph",
            node_attrs=('weight', ),
            edge_attrs=('weight', ),
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

        self._length = None

    def __del__(self):
        try:
            self._conn.close()
        except AttributeError:
            pass

    def __iter__(self):
        return iter(self._node)

    def __contains__(self, n):
        if isinstance(n, str) or isinstance(n, int):
            self._cur.execute(SQL_FACTORY["check_node_exists"], (n, ))
            return self._cur.fetchall()[0][0] >= 1
        return False

    def __len__(self):
        return self._node.__len__()

    def __getitem__(self, n):
        return self.adj[n]

    @not_permitted("Modifying graph is not permitted")
    def add_node(self, node_for_adding, **attr):
        pass

    @not_permitted("Modifying graph is not permitted")
    def add_nodes_from(self, nodes_for_adding, **attr):
        pass

    @not_permitted("Modifying graph is not permitted")
    def remove_node(self, n):
        pass

    @not_permitted("Modifying graph is not permitted")
    def remove_nodes_from(self, nodes):
        pass

    def number_of_nodes(self):
        return self.__len__()

    def order(self):
        return self.__len__()

    def has_node(self, n):
        return self.__contains__(n)

    @not_permitted("Modifying graph is not permitted")
    def add_edge(self, u_of_edge, v_of_edge, **attr):
        pass

    @not_permitted("Modifying graph is not permitted")
    def add_edges_from(self, ebunch_to_add, **attr):
        pass

    @not_permitted("Modifying graph is not permitted")
    def add_weighted_edges_from(self, ebunch_to_add, weight='weight', **attr):
        pass

    @not_permitted("Modifying graph is not permitted")
    def remove_edge(self, u, v):
        pass

    @not_permitted("Modifying graph is not permitted")
    def remove_edges_from(self, ebunch):
        pass

    @not_permitted("Modifying graph is not permitted")
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

    @not_permitted("Modifying graph is not permitted")
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

