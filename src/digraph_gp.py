import networkx as nx
from immutable_graph import ImmutableGraph
from dao import SuccDao, PredDao


class DiGraphGP(ImmutableGraph, nx.DiGraph):

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
        super(DiGraphGP, self).__init__(
            db_host, db_port, db_user, db_passwd, db_name, node_attrs, edge_attrs, **graph_attr
        )
        self._adj = SuccDao(self._db_config, self._edge_attrs)
        self._pred = PredDao(self._db_config, self._edge_attrs)  # predecessor
        self._succ = SuccDao(self._db_config, self._edge_attrs)  # successor

    def __del__(self):
        try:
            self._conn.close()
        except AttributeError:
            pass

    def adjacency(self):
        return self._adj.iter_items()

    def to_undirected(self, reciprocal=False, as_view=False):
        from graph_gp import GraphGP
        return GraphGP(
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
        return self.copy()

    def reverse(self, copy=True):
        return ReversedDiGraphGP(self)


class ReversedDiGraphGP(DiGraphGP):

    def __init__(self, original_graph):
        super(ReversedDiGraphGP, self).__init__(
            db_host=original_graph._db_config["host"],
            db_port=original_graph._db_config["port"],
            db_user=original_graph._db_config["user"],
            db_passwd=original_graph._db_config["password"],
            db_name=original_graph._db_config["database"],
            node_attrs=original_graph._node_attrs,
            edge_attrs=original_graph._edge_attrs,
            **original_graph.graph
        )
        if isinstance(original_graph._pred, PredDao):
            self._pred = SuccDao(self._db_config, self._edge_attrs)
            self._succ = PredDao(self._db_config, self._edge_attrs)
            self._adj = PredDao(self._db_config, self._edge_attrs)
        elif isinstance(original_graph._pred, SuccDao):
            self._adj = SuccDao(self._db_config, self._edge_attrs)
            self._pred = PredDao(self._db_config, self._edge_attrs)
            self._succ = SuccDao(self._db_config, self._edge_attrs)
        else:
            raise AttributeError("original_graph is not a valid directed graph")






