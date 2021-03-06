#!/usr/bin/env python3


import networkx as nx
from sql_factory import SQL_FACTORY
from dao import AdjDAO
from immutable_graph import ImmutableGraph


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

    def adjacency(self):
        return self._adj.iter_items()

    def to_directed(self, as_view=False):
        from digraph_gp import DiGraphGP
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

