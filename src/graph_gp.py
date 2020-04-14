#!/usr/bin/env python3


import networkx as nx
from sql_factory import SQL_FACTORY
from dao import AdjDAO
from immutable_graph import ImmutableGraph
from digraph_gp import DiGraphGP


class GraphGP(ImmutableGraph, nx.Graph):

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

        super(GraphGP, self).__init__(
            db_host, db_port, db_user, db_passwd, db_name, node_attrs, edge_attrs, **graph_attr
        )
        self._adj = AdjDAO(self._db_config, self._edge_attrs)

    def __del__(self):
        try:
            self._conn.close()
        except AttributeError:
            pass

    def has_edge(self, u, v):
        assert u.__class__.__name__ == v.__class__.__name__
        self._cur.execute(
            SQL_FACTORY["check_edge_exists"], (u, v, v, u)
        )
        return self._cur.fetchall()[0][0] >= 1

    def get_edge_data(self, u, v, default=None):
        assert u.__class__.__name__ == v.__class__.__name__
        self._cur.execute(
            SQL_FACTORY["query_edge"].replace("<attrs>", ', '.join(self._edge_attrs)), (u, v, v, u)
        )
        edge_data = self._cur.fetchone()
        return dict(zip(self._edge_attrs, edge_data[0])) if edge_data else default

    def adjacency(self):
        return self._adj.iter_items()

    def to_directed(self, as_view=False):
        return DiGraphGP(
            db_host=self._db_config["host"],
            db_port=self._db_config["port"],
            db_user=self._db_config["user"],
            db_passwd=self._db_config["password"],
            db_name=self._db_config["database"],
            node_attrs=self._node_attrs,
            edge_attrs=self._edge_attrs,
            **self.graph
        )

    def to_undirected(self, as_view=False):
        return self.copy()

