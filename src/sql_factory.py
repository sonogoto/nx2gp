#!/usr/bin/env python3


SQL_FACTORY = {
    "count_node": "SELECT COUNT(1) FROM vertices",
    "count_edge": "SELECT COUNT(1) FROM edges",
    "sum_edge_weight": "SELECT SUM(%s) FROM edges",
    "check_node_exists": "SELECT COUNT(1) FROM vertices WHERE id = %s",
    "check_edge_exists": "SELECT COUNT(1) FROM edges WHERE src_id = %s AND dst_id = %s",
    "query_node": "SELECT <attrs> FROM vertices WHERE id = %s",
    "query_edge": "SELECT %s FROM edges WHERE src_id = %s AND dst_id = %s",
    "query_adj": "SELECT edges.dst_id, <attrs> FROM edges WHERE edges.src_id = %s UNION \
                  SELECT edges.src_id, <attrs> FROM edges WHERE edges.dst_id = %s",
    "iter_node": "SELECT id FROM vertices",
    "iter_adj": "SELECT DISTINCT src_id FROM edges UNION SELECT DISTINCT dst_id FROM edges",
}
