:mod:`dse_graph`
================

.. module:: dse_graph

.. data:: __version_info__

   The version of the driver extension in a tuple format

.. data:: __version__

   The version of the driver extension in a string format

.. autoclass:: DseGraph

   .. autoattribute:: DSE_GRAPH_QUERY_LANGUAGE

   .. automethod:: create_execution_profile

   .. automethod:: query_from_traversal

   .. automethod:: traversal_source(session=None, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT)

.. autoclass:: DSESessionRemoteGraphConnection (session[, graph_name, execution_profile])
