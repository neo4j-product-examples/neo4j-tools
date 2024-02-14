from typing import Tuple, Union
from numpy.typing import ArrayLike
from graphdatascience import GraphDataScience
import pandas as pd


def _make_map(x):
    if type(x) == str:
        return x, x
    elif type(x) == tuple:
        return x
    else:
        raise Exception("Entry must of type string or tuple")


def _make_set_clause(prop_names: ArrayLike, element_name='n', item_name='rec'):
    clause_list = []
    for prop_name in prop_names:
        clause_list.append(f'{element_name}.{prop_name} = {item_name}.{prop_name}')
    return 'SET ' + ', '.join(clause_list)


def _make_node_merge_query(node_key_name: str, node_label: str, cols: ArrayLike):
    template = f'''UNWIND $recs AS rec\nMERGE(n:{node_label} {{{node_key_name}: rec.{node_key_name}}})'''
    prop_names = [x for x in cols if x != node_key_name]
    if len(prop_names) > 0:
        template = template + '\n' + _make_set_clause(prop_names)
    return template + '\nRETURN count(n) AS nodeLoadedCount'


def _make_rel_merge_query(source_target_labels: Union[Tuple[str, str], str],
                          source_node_key: Union[Tuple[str, str], str],
                          target_node_key: Union[Tuple[str, str], str],
                          rel_type: str,
                          cols: ArrayLike,
                          rel_key: str = None):
    source_target_label_map = _make_map(source_target_labels)
    source_node_key_map = _make_map(source_node_key)
    target_node_key_map = _make_map(target_node_key)

    merge_statement = f'MERGE(s)-[r:{rel_type}]->(t)'
    if rel_key is not None:
        merge_statement = f'MERGE(s)-[r:{rel_type} {{{rel_key}: rec.{rel_key}}}]->(t)'

    template = f'''\tUNWIND $recs AS rec
    MATCH(s:{source_target_label_map[0]} {{{source_node_key_map[0]}: rec.{source_node_key_map[1]}}})
    MATCH(t:{source_target_label_map[1]} {{{target_node_key_map[0]}: rec.{target_node_key_map[1]}}})\n\t''' + merge_statement
    prop_names = [x for x in cols if x not in [rel_key, source_node_key_map[1], target_node_key_map[1]]]
    if len(prop_names) > 0:
        template = template + '\n\t' + _make_set_clause(prop_names, 'r')
    return template + '\n\tRETURN count(r) AS relLoadedCount'


def chunks(xs, n: int = 10_000):
    """
    split an array-like objects into chunks of size n.

    Parameters
    -------
    :param n: int
        The size of chunk. The last chunk will be the remainder if there is one.
    """
    n = max(1, n)
    return [xs[i:i + n] for i in range(0, len(xs), n)]


def load_nodes(gds: GraphDataScience, node_df: pd.DataFrame, node_key_col: str, node_label: str,
               chunk_size: int = 10_000):
    """
    Load nodes from a dataframe.

    Parameters
    -------
    :param gds: GraphDataScience
        The graph datascience object referencing the database
    :param node_df: pd.DataFrame
        The dataframe containing node data
    :param node_key_col: str
        The column of the dataframe to use as the MERGE key property
    :param node_label: str
        The node label to use (only one allowed).
    :param chunk_size: int , default 10_000
        The chunk size to use when batching rows for loading
    """
    records = node_df.to_dict('records')
    print(f'======  loading {node_label} nodes  ======')
    total = len(records)
    print(f'staging {total:,} records')
    query = _make_node_merge_query(node_key_col, node_label, node_df.columns.copy())
    print(f'\nUsing This Cypher Query:\n```\n{query}\n```\n')
    cumulative_count = 0
    for recs in chunks(records, chunk_size):
        res = gds.run_cypher(query, params={'recs': recs})
        cumulative_count += res.iloc[0, 0]
        print(f'Loaded {cumulative_count:,} of {total:,} nodes')


def load_rels(gds: GraphDataScience,
              rel_df: pd.DataFrame,
              source_target_labels: Union[Tuple[str, str], str],
              source_node_key: Union[Tuple[str, str], str],
              target_node_key: Union[Tuple[str, str], str],
              rel_type: str,
              rel_key: str = None,
              chunk_size: int = 10_000):
    """
    Load relationships from a dataframe.

    Parameters
    -------
    :param gds: GraphDataScience
        The graph datascience object referencing the database
    :param rel_df: pd.DataFrame
        The dataframe containing relationship data
    :param source_target_labels: Union[Tuple[str, str], str]
        The source and target node labels to use.
        Can pass a single string if source and target nodes have the same labels,
        otherwise a tuple of the form (source_node_label, target_node_label)
    :param source_node_key: Union[Tuple[str, str], str]
        The column of the dataframe to use as the source node MERGE key property.
        Can optionally pass a tuple of the form (source_node_key_name, df_column_name) to map as appropriate if the
        column name is different
    :param target_node_key: Union[Tuple[str, str], str]
        The column of the dataframe to use as the target node MERGE key property.
        Can optionally pass a tuple of the form (target_node_key_name, df_column_name) to map as appropriate if the
        column name is different
    :param rel_type: str
        The relationship type to use (only one allowed).
    :param rel_key: str
        A key to distinguish unique parallel relationships.
        The default behavior of this function is to assume only one instance of a relationship type between two nodes.
        A duplicate insert will have the behavior of overriding the existing relationship.
        If this behavior is undesirable, and you want to allow multiple instances of the same relationship type between
        two nodes (a.k.a parallel relationships), provide this key to use for merging relationships uniquely
    :param chunk_size: int , default 10_000
        The chunk size to use when batching rows for loading
    """
    records = rel_df.to_dict('records')
    print(f'======  loading {rel_type} relationships  ======')
    total = len(records)
    print(f'staging {total:,} records')
    query = _make_rel_merge_query(source_target_labels, source_node_key,
                                  target_node_key, rel_type, rel_df.columns.copy(), rel_key)
    print(f'\nUsing This Cypher Query:\n```\n{query}\n```\n')
    cumulative_count = 0
    for recs in chunks(records, chunk_size):
        res = gds.run_cypher(query, params={'recs': recs})
        cumulative_count += res.iloc[0, 0]
        print(f'Loaded {cumulative_count:,} of {total:,} relationships')
