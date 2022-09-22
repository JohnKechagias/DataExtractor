from typing import Dict, Set, List



def dfs(graph: Dict, visited: List[int] = None, node: int = None) -> Set:
    if visited is None:
        visited = []
        node = 0

    if node not in visited:
        visited.append(node)
        for neighbour in graph[node]:
            dfs(graph, visited, neighbour)

    return visited
