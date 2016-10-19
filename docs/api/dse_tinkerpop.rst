:mod:`dse_tinkerpop` - Version Info
===================================

.. module:: dse_tinkerpop

.. data:: __version_info__

   The version of the driver extension in a tuple format

.. data:: __version__

   The version of the driver extension in a string format

.. autoclass:: DseGraph (session, graph_name[, execution_profile])

   .. automethod:: prepare_traversal_query

   .. automethod:: graph_traversal_source

   .. automethod:: execute_traversal(traversal[, trace=False][, execution_profile])

   .. automethod:: execute_traversal_async(traversal[, trace=False][, execution_profile])

.. autoclass:: DSESessionRemoteGraphConnection (session, graph_name[, execution_profile])
