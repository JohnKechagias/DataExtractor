from pathlib import Path
from typing import List, Set
from dfs import dfs



def create_table_metadata(tables_folder: Path):
    tables = []
    tables_headings = []
    tables_metadata = []
    graph = set()

    for file in tables_folder.iterdir():
        tables.append(file.name.replace('.csv', ''))
        tables_metadata.append([file.name])

        with open(file, 'r') as file:
            heading = file.readline()
            tables_headings.append(heading.split(','))

    graph, links_lookup = get_tables_graph(tables_headings)
    table_queue = dfs(graph)

    tables_metadata[0].append(0)

    for index in range(len(table_queue) - 1):
        link = (table_queue[index], table_queue[index + 1])
        cols = links_lookup[link]
        tables_metadata[index].append(cols[0])
        tables_metadata[index + 1].append(cols[1])

    tables_metadata[-1].append(0)

    return tables_metadata

def get_tables_graph(tables_headings: List[List[str]]) -> Set:
    graph = {}
    links_lookup = {}
    for index, heading in enumerate(tables_headings):
        for w_index, word in enumerate(heading):
            for o_index, o_heading in enumerate(tables_headings):
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
