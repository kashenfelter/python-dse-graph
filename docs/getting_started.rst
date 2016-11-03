Getting Started
===============

First, make sure you have the DSE Graph extension properly :doc:`installed <installation>`.

Configuring a Traversal Execution Profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The DSE Graph extension takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Graph Traversals execution requires a custom execution profile (to enable Gremlin-bytecode as query language). Here is
how to accomplish this this configuration:

.. code-block:: python


    from dse.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from dse_graph import DseGraph

    ep = DseGraph.create_execution_profile('graph_name')
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()

    g = DseGraph.traversal_source(session)  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph


If you want to change execution property defaults, please see the `Execution Profile documentation <http://docs.datastax.com/en/developer/python-driver/3.7/execution_profiles/>`_
for a more generalized discussion of the API. Graph traversal queries use the same execution profile defined for DSE graph. If you
need to change the default properties, please refer to this documentation page: `DSE Graph Queries <http://docs.datastax.com/en/developer/python-driver-dse/1.1/graph/>`_


Graph Traversal Queries via a DSE Session (Explicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traversal queries can be executed explicitly using `session.execute_graph` or `session.execute_graph_async`. These functions
return results as DSE graph types. If you are familiar to DSE queries or need async execution, you might prefer that way.

.. code-block:: python

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(g.V())
    for result in session.execute_graph(query):  # Execute a GraphTraversal and print results
        print result


Graph Traversal Queries via TinkerPop (Implicit Execution)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the :class:`dse_graph.DseGraph` class, you can build a GraphTraversalSource
that will execute queries on a DSE session. We call this *implicit execution* because it
doesn't rely on any particular function to execute the query. Everything is managed
internally by TinkerPop while traversing the graph and the results are TinkerPop types as well.

For example:

.. code-block:: python

    g = DseGraph.traversal_source(session)  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph


You can also create multiple GraphTraversalSources and use them with the same execution profile (for different graphs):

.. code-block:: python

    g_users = DseGraph.traversal_source(session, graph_name='users')
    g_drones = DseGraph.traversal_source(session, graph_name='drones')

    print g_users.V().toList()  # Traverse the users Graph
    print g_drones.V().toList()  # Traverse the drones Graph


Specify the Execution Profile explicitly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you don't want to change the default graph execution profile (`EXEC_PROFILE_GRAPH_DEFAULT`), you can register a new
one as usual and use it explicitly. Here is an example:


.. code-block:: python

    from dse.cluster import Cluster
    from dse_graph import DseGraph

    cluster = Cluster()
    ep = DseGraph.create_execution_profile('graph_name')
    cluster.add_execution_profile('graph_traversal', ep)
    session = cluster.connect()

    # implicit execution
    g = DseGraph.traversal_source(session, execution_profile='graph_traversal')  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph

    # explicit execution
    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(g.V())
    session.execute_graph(query, execution_profile='graph_traversal')
