from pathlib import Path
from typing import List, Set
from dfs import dfs



def create_tables_metadata(table_files_folder: Path):
    """ Create metadata for the specified table files.

    Args:
        table_files_folder: Path to the table files.
    """
    tables = []
    tables_headings = []
    tables_metadata = []
    graph = set()

    for file in table_files_folder.iterdir():
        tables.append(file.name.replace('.csv', ''))
        tables_metadata.append([file.name])

        with open(file, 'r') as file:
            heading = file.readline()
            tables_headings.append(heading.split(','))

    graph, links_lookup = get_tables_graph(tables_headings)
    table_queue = dfs(graph)

    # First tables matchings column is the first.
    tables_metadata[0].append(0)

    for index in range(len(table_queue) - 1):
        link = (table_queue[index], table_queue[index + 1])
        cols = links_lookup[link]
        tables_metadata[index].append(cols[0])
        tables_metadata[index + 1].append(cols[1])

    # We don't need to create a lookup table from the
    # last table.
    tables_metadata[-1].append(0)

    return tables_metadata

def get_tables_graph(tables_headings: List[List[str]]) -> Set:
    """ Returns a graph that represents the relation of the
    tables bases on their headings --- tries to find foreign
    keys relations.

    Args: tables_headings: The headings of each table.

    Return: The relationship graph and the specific link
        of each table. Ex. "(0, 1) : (1, 0)" means that the
        second col of the first table is the same as the
        first col of the second table.
    """
    graph = {}
    links_lookup = {}
    # Func's compexity is abizmanlabysmal but the n size is relatively
    # small so there is no real problem.
    for index, heading in enumerate(tables_headings):
        for w_index, word in enumerate(heading):
            for o_index, o_heading in enumerate(tables_headings):
                # Don't create circular links.
                if heading == o_heading:
                    break

                try:
                    i = o_heading.index(word)
                    if i > 1:
                        continue

                    links_lookup[(o_index, index)] = (i, w_index)

                    if index not in graph.keys():
                        graph[index] = [o_index]
                    else:
                        graph[index].append(o_index)

                    if o_index not in graph.keys():
                        graph[o_index] = [index]
                    else:
                        graph[o_index].append(index)
                except ValueError:
                    continue

    return (graph, links_lookup)
