Getting Started
===============

First, make sure you have the DSE Graph extension properly :doc:`installed <installation>`.

Changes in Execution Property Defaults
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The DSE Graph extension takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Please see the `Execution Profile documentation <http://datastax.github.io/python-driver/execution_profiles.html>`_
for a more generalized discussion of the API. Graph traversal queries use the same execution profile defined for DSE graph. If you
need to change the default properties, please refer to this documentation page: `DSE Graph Queries <http://docs.datastax.com/en/developer/python-driver-dse/v1.1/graph/>`_


Graph Traversal Queries via TinkerPop (Implicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the :class:`dse_graph.DseGraph` class, you can build a GraphTraversalSource
that will execute queries on a DSE session. We call this *implicit execution* because it
doesn't rely on any particular function to execute the query. Everything is managed
internally by TinkerPop while traversing the graph.

For example:

.. code-block:: python

    from dse.cluster import Cluster
    from dse_graph import DseGraph

    cluster = Cluster()
    session = cluster.connect()

    g = DseGraph.traversal_source(session, 'a_graph_name')  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph


Graph Traversal Queries via a DSE Session (Explicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traversal queries can also be executed explicitly using `session.execute_graph` or `session.execute_graph_async`. These
functions return results as DSE graph types instead of the TinkerPop ones. If you are familiar to DSE queries or need async execution,
you might prefer that way.


.. code-block:: python

    from dse.cluster import Cluster
    from dse_tinkerpop import DseGraph

    cluster = Cluster()
    session = cluster.connect()

    g = DseGraph.traversal_source(session, 'a_graph_name')
    query = DseGraph.query_from_traversal(g.V())
    for result in session.execute_graph(query):  # Execute a GraphTraversal and print results
        print result
