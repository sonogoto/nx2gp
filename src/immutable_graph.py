#!/usr/bin/env python3

from not_permitted import not_permitted
import psycopg2
from dao import NodeDAO


class ImmutableGraph:

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
        self._node = NodeDAO(self._db_config, self._node_attrs)

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

    @not_permitted("Modifying graph is not permitted")
    def clear(self):
        pass
