from typing import Dict, List



# A hamiltonian path is a path that visits each node
# exactly once.
def get_a_hamiltonian_path(
    graph: Dict
) -> List[str]:
    """ Returns a hamiltonian path of a given graph from
    any starting node.

    Args:
        graph: The graph

    Returns: The hamiltonian path, or 0 if a
        path couldn't be found.
    """
    starting_nodes = set()
    for node in range(len(graph.keys())):
        if node not in starting_nodes:
            starting_nodes.add(node)
            path = get_hamiltonian_path(graph, node)

            if path:
                return path
    return 0


def get_hamiltonian_path(
    graph: Dict,
    node: int, path:
    List[int] = None
) -> List[str]:
    """ Returns a hamiltonian path of a given graph from
    a starting node.

    Args:
        graph: The graph.
        node: The starting node.
        path: The current path of the algorithm.

    Returns: The hamiltonian path, or 0 if a
        path couldn't be found.
    """
    if path is None:
        path = []

    if node not in set(path):
        path.append(node)

        if len(path) == len(graph.keys()):
            return path

        for pt_next in graph.get(node, []):
            res_path = [i for i in path]
            candidate = get_hamiltonian_path(graph, pt_next, res_path)
            # Skip loop or dead end.
            if candidate is not None:
                return candidate
        return 0
