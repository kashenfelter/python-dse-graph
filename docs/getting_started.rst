Getting Started
===============

First, make sure you have the DSE TinkerPop extension properly :doc:`installed <installation>`.

Changes in Execution Property Defaults
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The TinkerPop extension takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Please see the `Execution Profile documentation <http://datastax.github.io/python-driver/execution_profiles.html>`_
for a more generalized discussion of the API. Graph traversal queries use the same execution profile defined for DSE graph. If you
need to change the default properties, please refer to this documentation page: `DSE Graph Queries <http://docs.datastax.com/en/developer/python-driver-dse/v1.1/graph/>`_


Graph Traversal Queries via TinkerPop (Implicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the :class:`dse_tinkerpop.DseGraph` class, you can build a GraphTraversalSource
that will execute queries on a DSE session. We call this *implicit execution* because it
doesn't rely on any particular function to execute the query. Everything
is managed internally by TinkerPop while traversing the graph.

For example:

.. code-block:: python

    from dse.cluster import Cluster
    from dse_tinkerpop import DseGraph

    cluster = Cluster()
    session = cluster.connect()

    dtk = DseGraph(session, 'a_graph_name')
    g = dtk.graph_traversal_source()  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph


Graph Traversal Queries via DseGraph (Explicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traversal queries can also be executed explicitly using :meth:`dse_tinkerpop.DseGraph.execute_traversal` or
:meth:`dse_tinkerpop.DseGraph.execute_traversal_async`. If you are familiar to DSE queries and ResulSet or need
async execution, you might prefer that way.


.. code-block:: python

    from dse.cluster import Cluster
    from dse_tinkerpop import DseGraph

    cluster = Cluster()
    session = cluster.connect()

    dtk = DseGraph(session, 'a_graph_name')
    g = dtk.graph_traversal_source()  # Build the GraphTraversalSource
    for v in dtk.execute_traversal(g.V()):  # Execute a GraphTraversal and print results
        print v
