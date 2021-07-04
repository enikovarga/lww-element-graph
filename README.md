# State-Based Last-Write-Win Element Graph CRDT

This code provides an implementation of a State-based Last-Write-Win Element Graph CRDT in Python. 

### Concepts

#### CRDT
> In distributed computing, a conflict-free replicated data type (CRDT) is a data structure which can be 
> replicated across multiple computers in a network, where the replicas can be updated independently and 
> concurrently without coordination between the replicas, and where it is always mathematically possible 
> to resolve inconsistencies that might come up. 
> 

#### State-based replication
> State-based CRDTs send their full local state to other replicas, where the states are merged by a function 
> which must be commutative, associative, and idempotent. 
>

#### Last-Write-Win
> LWW-based approach attaches a timestamp to each element. Consider add-set A and remove-set R, each containing 
> (element, timestamp) pairs. To add (resp. remove) an element e, add the pair (e, now()), where now was specified 
> earlier, to A (resp. to R). Merging two replicas takes the union of their add-sets and remove-sets. An element e 
> is in the set if it is in A, and it is not in R with a higher timestamp: lookup(e) = ∃ t, ∀ t 0 > t: (e,t) ∈ A ∧ (e,t0) / ∈ R). 
> Since it is based on LWW, this data type is convergent.
>

#### Graph
> A graph data structure consists of a set of vertices (also called nodes or points), together with a set of 
> unordered pairs of these vertices for an undirected graph or a set of ordered pairs for a directed graph. 
> These pairs are known as edges (also called links or lines). 
>

### Solution
This implementation is for an acyclic directed graph.

#### Graph properties

`vertices_added_set` <br />
Captures vertices in the graph. A dictionary where the key represents a vertex that was added to the graph and 
the value holds the timestamp of when it was added to the graph.


`vertices_removed_set` <br />
Captures vertices deleted from the graph. A dictionary where the key represents a vertex that was removed/deleted 
from the graph and the value holds the timestamp of when the vertex was removed from the graph.

`edges_added_set` <br />
Captures the edges added to the graph. A dictionary where the key represents the vertex where the edge originates from 
and the value is a nested dictionary where the keys represent the connected vertices and each value stores a 
timestamp of when the connection was added to the graph.

`edges_removed_set` <br />
Captures the edges removed/deleted from the graph. A dictionary where the key represents the vertex where the edge 
originates from and the value is a nested dictionary where the keys represent the connected vertices and each value 
stores a timestamp of when the connection was removed to the graph.


#### Operations

`addVertex(vertex)` <br />
Add a vertex to the graph.

`addEdge(from_vertex, to_vertex)` <br />
Add an edge to the graph. If the vertices referenced by the edge do not exists, add them to the graph.

`removeVertex(vertex)` <br />
Remove vertex from the graph. If the removed vertex is referenced by an edge, regardless of direction, 
the edge is also removed from the graph.

`removeEdge(from_vertex, to_vertex)` <br />
Remove edge from the graph.

`lookupVertexExists(vertex)` <br />
Look up whether a vertex exists in the graph.

`lookupConnectedVertices(vertex)` <br />
Look up vertices connected to a vertex. A vertex considered as a connection if an edge exists where
the vertex being searched for is the source of the connection and the connected vertex is at the ending
point of the edge.

`findPaths(from_vertex, to_vertex)` <br />
Find all possible paths between two vertices.

`merge(replica_vertices_added, replica_vertices_removed, replica_edges_added, replica_edges_removed)` <br />
Update local graph by merging it with the state of a replica.


### Set up and testing

#### Prerequisites

Python 3.6.* or later.

#### Virtual environment and dependencies

From the root directory of the project, create the virtual environment:

```
python3 -m venv ./venv
```

Activate the newly created virtual environment:

```
source ./venv/bin/activate
```

Install the packages required for the application to run:

```
pip3 install -r ./requirements.txt
```

#### Running the tests
```
pytest -vvs ./test
```
