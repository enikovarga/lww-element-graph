import time
from threading import RLock


class Graph:
    """
    A class to represent a directed Last-Write-Win Element Graph with functionality to add a vertex/edge,
    remove a vertex/edge, check if a vertex is in the graph, query for all vertices connected to a
    vertex, find any path between two vertices, and merge with concurrent changes from other replicas.
    """

    def __init__(self):
        self.vertices_added_set = {}
        self.vertices_removed_set = {}
        self.edges_added_set = {}
        self.edges_removed_set = {}
        self.lock = RLock()

    def addVertex(self, vertex, timestamp=None):
        """
        Add a vertex to the graph with timestamp. Timestamp can be passed to the method, otherwise
        current epoch time is used. If the vertex already exists, the timestamp is updated.

        :param vertex: vertex to be added
        :param timestamp: timestamp for the operation. Default is None
        """

        with self.lock:
            timestamp = timestamp if timestamp else self._generateTimestamp()
            self.vertices_added_set[vertex] = timestamp

    def addEdge(self, frm, to):
        """
        Add an edge to the graph with current epoch time. If the edge already exists,
        its timestamp is updated. If vertices referenced by the edge do not exists, the addVertex
        method is called and the vertex is added to the graph.

        :param frm: vertex the connection originates from
        :param to: vertex the connection points to
        """

        with self.lock:
            timestamp = self._generateTimestamp()

            if not self.lookupVertexExists(to):
                self.addVertex(to, timestamp)

            if not self.lookupVertexExists(frm):
                self.addVertex(frm, timestamp)

            if self.edges_added_set.get(frm):
                self.edges_added_set[frm][to] = timestamp
            else:
                self.edges_added_set[frm] = {to: timestamp}

    def removeVertex(self, vertex):
        """
        Remove vertex from the graph by adding it to the vertices_removed_set with timestamp.
        Where the removed vertex is referenced by an edge, regardless of direction, the removeEdge
        method is called and the edge is added to the edges_removed_set with the same timestamp
        as the removed vertex.

        :param vertex: vertex to be removed
        """

        with self.lock:
            timestamp = self._generateTimestamp()

            self.vertices_removed_set[vertex] = timestamp

            connected_vertices = list(self.edges_added_set.get(vertex, {}).keys())

            # deletes edges originating from removed vertex
            for connection in connected_vertices:
                self.removeEdge(vertex, connection, timestamp)

            # deletes edges pointing to removed vertex
            for connection in self.edges_added_set.keys():
                if connection != vertex:
                    if vertex in self.edges_added_set[connection].keys():
                        self.removeEdge(connection, vertex, timestamp)

    def removeEdge(self, frm, to, timestamp=None):
        """
        Remove edge from the graph by adding it to the edges_removed_set with timestamp.
        Timestamp can be passed to the method, otherwise current epoch time is used.
        If the edge already exists in the edges_removed_set, its timestamp is updated.

        :param frm: vertex the connection originates from
        :param to: vertex the connection points to
        :param timestamp: timestamp for the operation. Default is None
        """

        with self.lock:
            timestamp = timestamp if timestamp else self._generateTimestamp()

            if self.edges_removed_set.get(frm):
                self.edges_removed_set[frm][to] = timestamp
            else:
                self.edges_removed_set[frm] = {to: timestamp}

    def lookupVertexExists(self, vertex):
        """
        Look up whether vertex exists in the graph. If the vertex is present in the vertices_added_set
        and not present, or present with a lesser timestamp, in the vertices_removed_set, then vertex is
        considered to exist in the graph.
        Favouring the vertices_added_set if added and removed operations were concurrent and
        they hold the same timestamp value.

        :param vertex: vertex to look up in the graph
        :return: True is the vertex exists, False if not
        """

        if vertex in self.vertices_added_set.keys() and (
            vertex not in self.vertices_removed_set.keys()
            or self.vertices_added_set[vertex] >= self.vertices_removed_set[vertex]
        ):
            return True
        else:
            return False

    def lookupConnectedVertices(self, vertex):
        """
        Look up vertices connected to a vertex. A vertex considered as a connection if an edge exists where
        the vertex being searched for is the source of the connection and the connected vertex is at the ending
        point of the edge. The edge must not be present in the edges_removed_set or its timestamp must be greater
        in the edges_added_set.
        Favouring the edges_added_set if added and removed operations were concurrent and
        they hold the same timestamp value.


        :param vertex: vertex connections need to be looked up for
        :return: list of all connected vertices, empty list if vertex has no connected vertices
        """

        added_connections = self.edges_added_set.get(vertex)
        removed_connections = self.edges_removed_set.get(vertex)

        if added_connections is None:
            return []
        elif removed_connections is None:
            return list(added_connections.keys())
        else:
            connected = []

            removed_connection_vertices = list(removed_connections.keys())

            for connection in added_connections.keys():
                if (
                    connection not in removed_connection_vertices
                    or removed_connections[connection] <= added_connections[connection]
                ):
                    connected.append(connection)

            return connected

    def findPaths(self, vertex1, vertex2, path=[]):
        """
        Find all possible paths between two vertices.

        :param vertex1: vertex for starting point of the path
        :param vertex2: vertex for ending point of the path
        :param path: list of vertices to build a path
        :return: list of lists, where each sublist is considered a possible path between vertex1 and vertex2
        """

        path = path + [vertex1]

        if vertex1 == vertex2:
            return [path]

        connected_vertices = self.lookupConnectedVertices(vertex1)

        paths = []

        for vertex in connected_vertices:
            if vertex not in path:
                newpaths = self.findPaths(vertex, vertex2, path)

            try:
                for newpath in newpaths:
                    paths.append(newpath)
            except NameError:
                continue

        return paths

    def merge(
        self,
        received_vertices_added_set,
        received_vertices_removed_set,
        received_edges_added_set,
        received_edges_removed_set,
    ):
        """
        Update local graph by merging it with the state of a replica. Calls _mergeVertices and _mergeEdges methods
        to merge the replica's vertices_added_set, vertices_removed_set, edges_added_set and edges_removed_set
        into the local representation of these sets.

        :param received_vertices_added_set: vertices_added_set of the replica
        :param received_vertices_removed_set: vertices_removed_set of the replica
        :param received_edges_added_set: edges_added_set of the replica
        :param received_edges_removed_set: edges_removed_set of the replica
        """

        with self.lock:
            self.vertices_added_set = self._mergeVertices(
                self.vertices_added_set, received_vertices_added_set
            )
            self.vertices_removed_set = self._mergeVertices(
                self.vertices_removed_set, received_vertices_removed_set
            )

            self.edges_added_set = self._mergeEdges(
                self.edges_added_set, received_edges_added_set
            )
            self.edges_removed_set = self._mergeEdges(
                self.edges_removed_set, received_edges_removed_set
            )

    def _mergeVertices(self, local_set, received_set):
        """
        Merge two sets of vertices, by appending any new vertices present in the replica's graph to the
        local graph. If the vertex is present in both the local and replica graphs and its timestamp is
        greater in the replica graph, then the vertex in the local graph is updated with timestamp from
        the replica graph.

        :param local_set: vertices in the local graph
        :param received_set: vertices from the replica graph
        :return: merged set of vertices
        """

        latest_vertices_set = local_set.copy()

        overlapping_vertices = local_set.keys() & received_set.keys()

        for vertex in overlapping_vertices:
            if received_set[vertex] > local_set[vertex]:
                latest_vertices_set[vertex] = received_set[vertex]

        for vertex in received_set.keys() - overlapping_vertices:
            latest_vertices_set[vertex] = received_set[vertex]

        return latest_vertices_set

    def _mergeEdges(self, local_set, received_set):
        """
        Merge two sets of edges, by appending any new edges present in the replica's graph to the
        local graph. If the edge is present in both the local and the replica graphs and the edge's
        timestamp is greater in the replica graph, then the edge in the local graph is updated with
        the timestamp from the replica graph.

        :param local_set: edges in the local graph
        :param received_set: edges in the replica graph
        :return: merged set of edges
        """

        latest_edges_set = local_set.copy()

        overlapping_vertices = local_set.keys() & received_set.keys()

        for vertex in overlapping_vertices:
            for edge in latest_edges_set[vertex].keys():
                if (
                    edge in latest_edges_set[vertex].keys()
                    and received_set[vertex][edge] > local_set[vertex][edge]
                ):
                    latest_edges_set[vertex][edge] = received_set[vertex][edge]
                elif edge not in latest_edges_set[vertex].keys():
                    latest_edges_set[vertex][edge] = received_set[vertex][edge]

        for vertex in received_set.keys() - overlapping_vertices:
            latest_edges_set[vertex] = received_set[vertex]

        return latest_edges_set

    def _generateTimestamp(self):
        """
        Generate current epoch time in microseconds.

        :return: epoch time
        """

        return int(time.time() * 1000000)
