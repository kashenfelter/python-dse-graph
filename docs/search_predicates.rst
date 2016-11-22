Search
======

DSE Graph can use search indexes that take advantage of DSE Search functionality for efficient traversal queries. Here is the list of additional search predicates:

Text tokenization:

* :func:`token <dse_graph.predicates.Search.token>`
* :func:`token_prefix <dse_graph.predicates.Search.token_prefix>`
* :func:`token_regex <dse_graph.predicates.Search.token_regex>`

Text match:

* :func:`prefix <dse_graph.predicates.Search.prefix>`
* :func:`regex <dse_graph.predicates.Search.regex>`

Geo:

* :func:`inside <dse_graph.predicates.Geo.inside>`

Create search indexes
~~~~~~~~~~~~~~~~~~~~~

For text tokenization:

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('text_field').asText().add()")

For text match:

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('text_field').asString().add()")


For geospatial:

You can create a geospatial index on Point and LineString fields.

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('point_field').add()")


Using search indexes
~~~~~~~~~~~~~~~~~~~~

Token:

.. code-block:: python

    from dse_graph.predicates import Search
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','text_field', Search.token_regex('Hello.+World')).values('text_field'))
    session.execute_graph(query)

Text:

.. code-block:: python

    from dse_graph.predicates import Search
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','text_field', Search.prefix('Hello')).values('text_field'))
    session.execute_graph(query)

Geospatial:

.. code-block:: python

    from dse_graph.predicates import Geo
    from dse.util import Distance
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','point_field', Geo.inside(Distance(46, 71, 100)).values('point_field'))
    session.execute_graph(query)


For more details, please refer to the official `DSE Search Indexes Documentation <https://docs.datastax.com/en/latest-dse/datastax_enterprise/graph/using/useSearchIndexes.html>`_
