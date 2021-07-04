from datetime import datetime
from time import sleep


def test_addVertex(graph):
    vertex = 1

    graph.addVertex(vertex)

    ts = graph.vertices_added_set.get(vertex)

    assert vertex in graph.vertices_added_set.keys()
    assert isinstance(datetime.fromtimestamp(ts/1000000), datetime)


def test_addEdge(graph):
    connection = [1, 3]

    graph.addEdge(connection[0], connection[1])

    e_ts = graph.edges_added_set.get(connection[0], {}).get(connection[1])
    v_ts1 = graph.vertices_added_set.get(connection[0])
    v_ts2 = graph.vertices_added_set.get(connection[1])

    assert connection[1] in graph.edges_added_set.get(connection[0], {}).keys()

    assert connection[0] in graph.vertices_added_set.keys()
    assert connection[1] in graph.vertices_added_set.keys()

    assert isinstance(datetime.fromtimestamp(e_ts/1000000), datetime)
    assert e_ts == v_ts1 == v_ts2


def test_removeVertex(graph):
    vertex_to_remove = 1
    connected_vertex1 = 2
    connected_vertex2 = 3

    graph.addEdge(vertex_to_remove, connected_vertex1)
    graph.addEdge(connected_vertex2, vertex_to_remove)

    graph.removeVertex(vertex_to_remove)

    assert graph.vertices_removed_set.get(vertex_to_remove) > graph.vertices_added_set.get(vertex_to_remove)

    assert vertex_to_remove in graph.edges_removed_set.keys()
    assert connected_vertex1 in graph.edges_removed_set.get(vertex_to_remove, {}).keys()
    assert vertex_to_remove in graph.edges_removed_set.get(connected_vertex2, {}).keys()

    assert graph.vertices_removed_set.get(vertex_to_remove) > graph.vertices_added_set.get(vertex_to_remove)


def test_removeEdge(graph):
    connection = [1, 3]

    graph.addEdge(connection[0], connection[1])

    graph.removeEdge(connection[0], connection[1])

    assert graph.edges_removed_set.get(connection[0], {}).get(connection[1])


def test_lookupVertexExists(graph):
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
    vertex = 1
    connected_vertex1 = 2
    connected_vertex2 = 3

    graph.addEdge(vertex, connected_vertex1)
    graph.addEdge(vertex, connected_vertex2)
    assert graph.lookupConnectedVertices(vertex) == [connected_vertex1, connected_vertex2]

    graph.removeEdge(vertex, connected_vertex1)
    assert graph.lookupConnectedVertices(vertex) == [connected_vertex2]

    non_existent_vertex = 5
    assert graph.lookupConnectedVertices(non_existent_vertex) == []


def test_findPaths(graph):
    connections = [[1, 2], [1, 3], [2, 5], [3, 5],  [5, 7], [4, 6]]

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

    graph.merge(replica_graph.vertices_added_set,
                replica_graph.vertices_removed_set,
                replica_graph.edges_added_set,
                replica_graph.edges_removed_set)

    assert graph.vertices_added_set.keys() == {1, 2, 3, 5}
    assert graph.vertices_removed_set.keys() == {1, 3}

    assert graph.edges_added_set.keys() == {1, 2, 3}
    assert graph.edges_removed_set.keys() == {1, 3}


def test_merge2(graph, replica_graph):
    connection = [1, 2]

    graph.addEdge(connection[0], connection[1])
    sleep(1)
    replica_graph.addEdge(connection[0], connection[1])
    sleep(1)
    replica_graph.removeEdge(connection[0], connection[1])

    graph.merge(replica_graph.vertices_added_set,
                replica_graph.vertices_removed_set,
                replica_graph.edges_added_set,
                replica_graph.edges_removed_set)

    # check timestamp is updated for matching keys
    assert graph.edges_added_set[1][2] == replica_graph.edges_added_set[1][2]
    # check removed_set copied
    assert graph.edges_removed_set == replica_graph.edges_removed_set


def test_merge3(graph, replica_graph):
    vertex = 1

    replica_graph.addVertex(vertex)
    sleep(1)
    graph.addVertex(vertex)
    sleep(1)
    replica_graph.removeVertex(vertex)

    graph.merge(replica_graph.vertices_added_set,
                replica_graph.vertices_removed_set,
                replica_graph.edges_added_set,
                replica_graph.edges_removed_set)

    # check added_vertex_set ts is kept as is
    assert graph.vertices_added_set[vertex] > replica_graph.vertices_added_set[vertex]
    # check removed_vertex_set is copied
    assert graph.vertices_removed_set == replica_graph.vertices_removed_set
