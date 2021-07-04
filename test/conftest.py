import pytest
from main.lww_directed_graph import Graph


@pytest.fixture(scope="function")
def graph():
    return Graph()


@pytest.fixture(scope="function")
def replica_graph():
    return Graph()
