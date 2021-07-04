import time


class Lww:
    """
    A class to represent a Last-Write-Win Element Graph with functionalities to add a vertex/edge,
    remove a vertex/edge, check if a vertex is in the graph, query for all vertices connected to a
    vertex, find any path between two vertices, and merge with concurrent changes from other replicas.
    """

    def __init__(self):
        self.vertices_added_set = {}
        self.vertices_removed_set = {}
        self.edges_added_set = {}
        self.edges_removed_set = {}

    def addVertex(self, vertex, timestamp=None):

        timestamp = timestamp if timestamp else int(time.time() * 1000000)

        self.vertices_added_set[vertex] = timestamp

    def addEdge(self, to, frm):

        timestamp = int(time.time() * 1000000)

        # TODO: test vertex is added
        if not self.lookupVertexExists(to):
            self.addVertex(to, timestamp)

        if not self.lookupVertexExists(frm):
            self.addVertex(frm, timestamp)

        if self.edges_added_set.get(to):
            self.edges_added_set[to].append([frm, timestamp])
        else:
            self.edges_added_set[to] = [[frm, timestamp]]

        if self.edges_added_set.get(frm):
            self.edges_added_set[frm].append([to, timestamp])
        else:
            self.edges_added_set[frm] = [[to, timestamp]]

    def removeVertex(self, vertex):

        timestamp = int(time.time() * 1000000)

        self.vertices_removed_set[vertex] = timestamp

        connected_vertices = list(list(zip(*self.edges_added_set.get(vertex))))

        # TODO: test delete edges
        for connection in connected_vertices:
            self.removeEdge(connection, vertex, timestamp)

    def removeEdge(self, to, frm, timestamp=None):

        timestamp = timestamp if timestamp else int(time.time() * 1000000)

        if self.edges_removed_set.get(to):
            self.edges_removed_set[to].append([frm, timestamp])
        else:
            self.edges_removed_set[to] = [[frm, timestamp]]

        if self.edges_removed_set.get(frm):
            self.edges_removed_set[frm].append([to, timestamp])
        else:
            self.edges_removed_set[frm] = [[to, timestamp]]

    def lookupVertexExists(self, vertex):

        if vertex in self.vertices_added_set and (
            vertex not in self.vertices_removed_set
            or self.vertices_added_set[vertex] > self.vertices_removed_set[vertex]
        ):
            return True
        else:
            return False

    def lookupConnectedVertices(self, vertex):

        added_connections = self.edges_added_set.get(vertex)
        removed_connections = self.edges_removed_set.get(vertex)

        if added_connections is None:
            return []
        elif removed_connections is None:
            return list(list(zip(*added_connections))[0])
        else:
            connected = []

            removed_connection_vertices = list(list(zip(*removed_connections))[0])

            for connection in added_connections:
                if connection[0] not in removed_connection_vertices:
                    connected.append(connection[0])
                else:
                    for r_connection in removed_connections:
                        if (
                            connection[0] == r_connection[0]
                            and connection[1] > r_connection[1]
                        ):
                            connected.append(connection[0])

            return connected

    def findpaths(self, vertex1, vertex2, path=[]):
        print("hello")

    def merge(self, received_state):
        # TODO: consider concurrent transactions
        print("hello")
