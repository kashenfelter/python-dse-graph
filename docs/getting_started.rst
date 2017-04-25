Getting Started
===============

First, make sure you have the DSE Graph extension properly :doc:`installed <installation>`.

Configuring a Traversal Execution Profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The DSE Graph extension takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Graph Traversals execution requires a custom execution profile (to enable Gremlin-bytecode as query language). Here is
how to accomplish this configuration:

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

Graph Traversal Queries
~~~~~~~~~~~~~~~~~~~~~~~

The base DSE driver provides `Session.execute_graph`, which allows users to execute traversal query strings.
To take an example from the DSE driver documentation,

.. code-block:: python

    from dse.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
    from dse.graph import GraphOptions

    # create the default execution profile pointing at a specific graph
    graph_name = 'test'
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()

    # use the system execution profile (or one with no graph_options.graph_name set) when accessing the system API
    session.execute_graph("system.graph(name).ifNotExists().create();", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)

    # allow creation of vertices with label 'user'...
    session.execute_graph("schema.vertexLabel('user').ifNotExists().create();")
    # create a property key 'name' with type Text...
    session.execute_graph("schema.propertyKey('name').Text().ifNotExists().create();")
    # and allow 'user' vertices to have a 'name' label
    session.execute_graph("schema.vertexLabel('user').properties('name').add()")

    # Create an 'age' label for the 'user' vertex label as well
    session.execute_graph("schema.propertyKey('age').Int().ifNotExists().create();")
    session.execute_graph("schema.vertexLabel('user').properties('age').add()")

    # Use the default execution profile to create a new 'user' vertex
    result = session.execute_graph('g.addV(label, "user", "name", "John", "age", "35")')

For more details on using TinkerPop, see `the gremlin-python documentation
<http://tinkerpop.apache.org/docs/current/reference/#gremlin-python>`_.

This module provides a Python API for specifying graph traversal with TinkerPop.
These native traversal queries can be executed explicitly, with a DSE `Session` object,
or implicitly

Explicit Graph Traversal Excution with a DSE Session
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Traversal queries can be executed explicitly using `session.execute_graph` or `session.execute_graph_async`. These functions
return results as DSE graph types. If you are familiar with DSE queries or need async execution, you might prefer that way.
Below is an example of explicit execution. For this example, assume the schema has been generated as above:

.. code-block:: python

    from dse_graph import DseGraph
    from pprint import pprint

    # create a tinkerpop graphson2 ExecutionProfile
    ep = DseGraph.create_execution_profile('graph_name')
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()

    g = DseGraph.traversal_source(session=session)
    addV_query = DseGraph.query_from_traversal(
        g.addV('user').property('name', 'Preeta').property('age', 32)
    )
    V_query = DseGraph.query_from_traversal(g.V())

    for result in session.execute_graph(addV_query):
        pprint(result.value)
    for result in session.execute_graph(V_query):
        pprint(result.value)

Implicit Graph Traversal Execution with TinkerPop
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using the :class:`dse_graph.DseGraph` class, you can build a GraphTraversalSource
that will execute queries on a DSE session without explicitly passing anything to
that session. We call this *implicit execution* because the `Session` is not
explicitly involved. Everything is managed internally by TinkerPop while
traversing the graph and the results are TinkerPop types as well.

For example:

.. code-block:: python

    # Build the GraphTraversalSource
    g = DseGraph.traversal_source(session)
    # implicitly execute the query by traversing the TraversalSource
    g.addV('user').property('name', 'Preeta').property('age', 32).toList()
    # view the results of the execution
    pprint(g.V().toList())

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

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(g.V())
    session.execute_graph(query, execution_profile='graph_traversal')

You can also create multiple GraphTraversalSources and use them with the same execution profile (for different graphs):

.. code-block:: python

    g_users = DseGraph.traversal_source(session, graph_name='users', ep)
    g_drones = DseGraph.traversal_source(session, graph_name='drones', ep)

    print g_users.V().toList()  # Traverse the users Graph
    print g_drones.V().toList()  # Traverse the drones Graph
