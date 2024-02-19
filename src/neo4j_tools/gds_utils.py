from graphdatascience import GraphDataScience


def clear_all_gds_graphs(gds: GraphDataScience):
    g_names = gds.graph.list().graphName.tolist()
    for g_name in g_names:
        g = gds.graph.get(g_name)
        g.drop()


def delete_relationships(rel_type: str, gds: GraphDataScience, batch_size: int = 1000, src_node_label: str = None):
    src_node_filter = ''
    if src_node_label is not None:
        src_node_filter = ':' + src_node_label
    gds.run_cypher(f'''
        MATCH({src_node_filter})-[r:{rel_type}]->()
        CALL {{
            WITH r
            DELETE r
        }} IN TRANSACTIONS OF {batch_size} ROWS
        ''')


def delete_nodes(node_label: str, gds: GraphDataScience, batch_size: int = 1000):
    gds.run_cypher(f'''
        MATCH(n:{node_label})
        CALL {{
            WITH n
            DETACH DELETE n
        }} IN TRANSACTIONS OF {batch_size} ROWS
        ''')
