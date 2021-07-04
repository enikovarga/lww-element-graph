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
        Add a vertex to the graph with timestamp. Timestamp can either be passed to the method, if not
        passed then current epoch timestamp is used. If the vertex already exists, the timestamp is updated.
        Lock is acquired by the thread to prevent data race in case concurrent processes are running.

        :param vertex: vertex to be added
        :param timestamp: timestamp for the operation
        :return: None
        """

        with self.lock:
            timestamp = timestamp if timestamp else self._generateTimestamp()
            self.vertices_added_set[vertex] = timestamp


    def addEdge(self, frm, to):
        """
        Add an edge/arc to the directed graph with current epoch timestamp.
        If vertices referenced by the edge do not exists, the addVertex method
        is called and the vertex is added to the graph.
        Lock is acquired by the thread to prevent data race in case concurrent processes are running.

        :param frm: vertex
        :param to:
        :return:
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

        :param vertex:
        :return:
        """

        with self.lock:
            timestamp = self._generateTimestamp()

            self.vertices_removed_set[vertex] = timestamp

            connected_vertices = list(self.edges_added_set.get(vertex, {}).keys())

            # deletes edges from removed vertex
            for connection in connected_vertices:
                self.removeEdge(vertex, connection, timestamp)

            # deleted edges pointing to removed vertex
            for connection in self.edges_added_set.keys():
                if connection != vertex:
                    if vertex in self.edges_added_set[connection].keys():
                        self.removeEdge(connection, vertex, timestamp)


    def removeEdge(self, frm, to, timestamp=None):
        """

        :param frm:
        :param to:
        :param timestamp:
        :return:
        """

        with self.lock:
            timestamp = timestamp if timestamp else self._generateTimestamp()

            if self.edges_removed_set.get(frm):
                self.edges_removed_set[frm][to] = timestamp
            else:
                self.edges_removed_set[frm] = {to: timestamp}


    def lookupVertexExists(self, vertex):
        """

        :param vertex:
        :return:
        """

        # favouring added set if added and removed values are concurrent
        if vertex in self.vertices_added_set.keys() and \
                (vertex not in self.vertices_removed_set.keys() or
                 self.vertices_added_set[vertex] >= self.vertices_removed_set[vertex]):
            return True
        else:
            return False


    def lookupConnectedVertices(self, vertex):
        """

        :param vertex:
        :return:
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

            # favouring added set if added and removed values are concurrent
            for connection in added_connections.keys():
                if connection not in removed_connection_vertices or removed_connections[connection] <= added_connections[connection]:
                    connected.append(connection)

            return connected


    def findPaths(self, vertex1, vertex2, path=[]):
        """

        :param vertex1:
        :param vertex2:
        :param path:
        :return:
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


    def merge(self, received_vertices_added_set, received_vertices_removed_set,
              received_edges_added_set, received_edges_removed_set):
        """

        :param received_vertices_added_set:
        :param received_vertices_removed_set:
        :param received_edges_added_set:
        :param received_edges_removed_set:
        :return:
        """

        with self.lock:
            self.vertices_added_set = self._mergeVertices(self.vertices_added_set, received_vertices_added_set)
            self.vertices_removed_set = self._mergeVertices(self.vertices_removed_set, received_vertices_removed_set)

            self.edges_added_set = self._mergeEdges(self.edges_added_set, received_edges_added_set)
            self.edges_removed_set = self._mergeEdges(self.edges_removed_set, received_edges_removed_set)


    def _mergeVertices(self, local_set, received_set):
        """

        :param local_set:
        :param received_set:
        :return:
        """

        latest_vertices_set = local_set.copy()

        overlapping_vertices = local_set.keys() & received_set.keys()

        for vertex in overlapping_vertices:
            if received_set[vertex] > local_set[vertex]:
                latest_vertices_set[vertex] = received_set[vertex]

        for vertex in (received_set.keys() - overlapping_vertices):
            latest_vertices_set[vertex] = received_set[vertex]

        return latest_vertices_set


    def _mergeEdges(self, local_set, received_set):
        """

        :param local_set:
        :param received_set:
        :return:
        """

        latest_edges_set = local_set.copy()

        overlapping_vertices = local_set.keys() & received_set.keys()

        for vertex in overlapping_vertices:
            for edge in latest_edges_set[vertex].keys():
                if edge in latest_edges_set[vertex].keys() and received_set[vertex][edge] > local_set[vertex][edge]:
                    latest_edges_set[vertex][edge] = received_set[vertex][edge]
                elif edge not in latest_edges_set[vertex].keys():
                    latest_edges_set[vertex][edge] = received_set[vertex][edge]

        for vertex in (received_set.keys() - overlapping_vertices):
            latest_edges_set[vertex] = received_set[vertex]

        return latest_edges_set


    def _generateTimestamp(self):
        """

        :return:
        """

        return int(time.time() * 1000000)
