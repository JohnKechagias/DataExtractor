from typing import Dict, List



def dfs(graph: Dict, visited: List[int] = None, node: int = None) -> List[int]:
    """ Dfs algorithm. https://en.wikipedia.org/wiki/Depth-first_search

    Args:
        graph: The graph in which to run the algorithm on.
        visited: List if visited nodes.
        node: The starting node.

    Returns:
        A list with all the visited nodes (ordered).
    """
    if visited is None:
        visited = []
        node = 0

    if node not in visited:
        visited.append(node)
        for neighbour in graph[node]:
            dfs(graph, visited, neighbour)

    return visited
