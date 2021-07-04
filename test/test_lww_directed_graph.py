from datetime import datetime
from time import sleep


def test_addVertex(graph):
    """Test to validate vertex is added to the graph and its timestamp is a valid timestamp value"""
    vertex = 1

    graph.addVertex(vertex)

    ts = graph.vertices_added_set.get(vertex)

    assert vertex in graph.vertices_added_set.keys()
    assert isinstance(datetime.fromtimestamp(ts / 1000000), datetime)


def test_addEdge(graph):
    """
    Test to validate:
    * edge is added to the graph and its timestamp is a valid timestamp value
    * both vertices referenced in the edge are also added to the graph
    * timestamp value of the edge and the vertices are the same
    """
    connection = [1, 3]

    graph.addEdge(connection[0], connection[1])

    e_ts = graph.edges_added_set.get(connection[0], {}).get(connection[1])
    v1_ts = graph.vertices_added_set.get(connection[0])
    v2_ts = graph.vertices_added_set.get(connection[1])

    assert connection[1] in graph.edges_added_set.get(connection[0], {}).keys()
    assert isinstance(datetime.fromtimestamp(e_ts / 1000000), datetime)

    assert connection[0] in graph.vertices_added_set.keys()
    assert connection[1] in graph.vertices_added_set.keys()

    assert e_ts == v1_ts == v2_ts


def test_removeVertex(graph):
    """
    Test to validate:
    * vertex is added to the removed_set with a greater timestamp than the vertex in the added_set
    * edges referencing the vertex, in either direction, are also added to the removed_set
    """
    vertex_to_remove = 1
    connected_vertex1 = 2
    connected_vertex2 = 3

    graph.addEdge(vertex_to_remove, connected_vertex1)
    graph.addEdge(connected_vertex2, vertex_to_remove)

    graph.removeVertex(vertex_to_remove)

    assert graph.vertices_removed_set.get(
        vertex_to_remove
    ) > graph.vertices_added_set.get(vertex_to_remove)

    assert vertex_to_remove in graph.edges_removed_set.keys()
    assert connected_vertex1 in graph.edges_removed_set.get(vertex_to_remove, {}).keys()
    assert vertex_to_remove in graph.edges_removed_set.get(connected_vertex2, {}).keys()


def test_removeEdge(graph):
    """
    Test to validate edge is added to the removed_set with a greater timestamp than the added_set
    """
    connection = [1, 3]

    graph.addEdge(connection[0], connection[1])

    graph.removeEdge(connection[0], connection[1])

    assert graph.edges_removed_set.get(connection[0], {}).get(
        connection[1]
    ) > graph.edges_added_set.get(connection[0], {}).get(connection[1])


def test_lookupVertexExists(graph):
    """
    Test to validate:
    * method returns True if vertex is in the added_set
    * method returns False if vertex is in the removed_set with a greater timestamp than the added_set
    * method returns True when the vertex is re-added to the graph which updates its timestamp in the added_set
    """
    vertex = 1

    graph.addVertex(vertex)
    assert graph.lookupVertexExists(vertex) is True

    graph.removeVertex(vertex)
    assert graph.lookupVertexExists(vertex) is False

    # re-adding vertex
    graph.addVertex(vertex)
    assert graph.lookupVertexExists(vertex) is True

    # negative test case
    non_existent_vertex = 2
    assert graph.lookupVertexExists(non_existent_vertex) is False


def test_lookupConnectedVertices(graph):
    """
    Test to validate:
    * all connected vertices present in the added_set are returned
    * if a vertex is in the removed_set, it is not considered a connected vertex
    * empty list is returned when the vertex does not exists in the graph
    """
    vertex = 1
    connected_vertex1 = 2
    connected_vertex2 = 3

    graph.addEdge(vertex, connected_vertex1)
    graph.addEdge(vertex, connected_vertex2)
    assert graph.lookupConnectedVertices(vertex) == [
        connected_vertex1,
        connected_vertex2,
    ]

    graph.removeEdge(vertex, connected_vertex1)
    assert graph.lookupConnectedVertices(vertex) == [connected_vertex2]

    non_existent_vertex = 5
    assert graph.lookupConnectedVertices(non_existent_vertex) == []


def test_findPaths(graph):
    """
    Test to validate all possible paths are returned between vertices and an empty list is returned
    if no paths are available between the vertices or the vertices do not exist in the graph.
    """
    connections = [[1, 2], [1, 3], [2, 5], [3, 5], [5, 7], [4, 6]]

    for connection in connections:
        graph.addEdge(connection[0], connection[1])

    paths = graph.findPaths(1, 3)
    assert paths == [[1, 3]]

    paths = graph.findPaths(1, 5)
    assert paths == [[1, 2, 5], [1, 3, 5]]

    paths = graph.findPaths(1, 7)
    assert paths == [[1, 2, 5, 7], [1, 3, 5, 7]]

    paths = graph.findPaths(1, 6)
    assert paths == []

    paths = graph.findPaths(8, 9)
    assert paths == []

    paths = graph.findPaths(5, 1)
    assert paths == []


def test_merge(graph, replica_graph):
    """
    Test to validate local vertices/edges are merged with vertices/edges from other replicas.
    No overlapping vertices/edges.
    """
    local_connections = [[1, 2], [2, 5]]
    replica_connections = [[3, 5]]
    removed_local_vertex = 1
    removed_replica_vertex = 3

    for connection in local_connections:
        graph.addEdge(connection[0], connection[1])

    for connection in replica_connections:
        replica_graph.addEdge(connection[0], connection[1])

    graph.removeVertex(removed_local_vertex)
    replica_graph.removeVertex(removed_replica_vertex)

    graph.merge(
        replica_graph.vertices_added_set,
        replica_graph.vertices_removed_set,
        replica_graph.edges_added_set,
        replica_graph.edges_removed_set,
    )

    assert graph.vertices_added_set.keys() == {1, 2, 3, 5}
    assert graph.vertices_removed_set.keys() == {1, 3}

    assert graph.edges_added_set.keys() == {1, 2, 3}
    assert graph.edges_removed_set.keys() == {1, 3}


def test_merge2(graph, replica_graph):
    """
    Test to validate local vertices/edges are merged with vertices/edges from other replicas.
    Edge deleted in the replica with greater timestamp.
    """
    connection = [1, 2]

    graph.addEdge(connection[0], connection[1])
    sleep(1)
    replica_graph.addEdge(connection[0], connection[1])
    sleep(1)
    replica_graph.removeEdge(connection[0], connection[1])

    graph.merge(
        replica_graph.vertices_added_set,
        replica_graph.vertices_removed_set,
        replica_graph.edges_added_set,
        replica_graph.edges_removed_set,
    )

    # check timestamp is updated for matching keys
    assert graph.edges_added_set[1][2] == replica_graph.edges_added_set[1][2]
    # check removed_set copied
    assert graph.edges_removed_set == replica_graph.edges_removed_set
    # validate edge does not exists in local graph
    assert graph.lookupConnectedVertices(connection[0]) == []


def test_merge3(graph, replica_graph):
    """
    Test to validate local vertices/edges are merged with vertices/edges from other replicas.
    Vertex deleted in the replica with greater timestamp.
    """
    vertex = 1

    replica_graph.addVertex(vertex)
    sleep(1)
    graph.addVertex(vertex)
    sleep(1)
    replica_graph.removeVertex(vertex)

    graph.merge(
        replica_graph.vertices_added_set,
        replica_graph.vertices_removed_set,
        replica_graph.edges_added_set,
        replica_graph.edges_removed_set,
    )

    # check added_vertex_set ts is kept as is
    assert graph.vertices_added_set[vertex] > replica_graph.vertices_added_set[vertex]
    # check removed_vertex_set is copied
    assert graph.vertices_removed_set == replica_graph.vertices_removed_set
    # validate vertex does not exists in local graph
    assert graph.lookupVertexExists(vertex) is False


def test_merge4(graph, replica_graph):
    """
    Test to validate local vertices/edges are merged with vertices/edges from other replicas.
    Edge deleted in the replica with lesser timestamp.
    """
    connection = [1, 2]

    replica_graph.addEdge(connection[0], connection[1])
    sleep(1)
    replica_graph.removeEdge(connection[0], connection[1])
    sleep(1)
    graph.addEdge(connection[0], connection[1])

    graph.merge(
        replica_graph.vertices_added_set,
        replica_graph.vertices_removed_set,
        replica_graph.edges_added_set,
        replica_graph.edges_removed_set,
    )

    # check timestamp of local graph is not updated with timestamp from replica
    assert graph.edges_added_set[1][2] != replica_graph.edges_added_set[1][2]
    # check removed_set copied
    assert graph.edges_removed_set == replica_graph.edges_removed_set
    # validate edge exists in local graph
    assert graph.lookupConnectedVertices(connection[0]) == [connection[1]]


def test_merge5(graph, replica_graph):
    """
    Test to validate local vertices/edges are merged with vertices/edges from other replicas.
    Vertex deleted in the replica with lesser timestamp.
    """
    vertex = 1

    replica_graph.addVertex(vertex)
    sleep(1)
    replica_graph.removeVertex(vertex)
    sleep(1)
    graph.addVertex(vertex)

    graph.merge(
        replica_graph.vertices_added_set,
        replica_graph.vertices_removed_set,
        replica_graph.edges_added_set,
        replica_graph.edges_removed_set,
    )

    # check added_vertex_set ts is kept as is
    assert graph.vertices_added_set[vertex] > replica_graph.vertices_added_set[vertex]
    # check removed_vertex_set is copied
    assert graph.vertices_removed_set == replica_graph.vertices_removed_set
    # validate vertex exists in local graph
    assert graph.lookupVertexExists(vertex) is True
