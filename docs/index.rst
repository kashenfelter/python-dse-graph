DataStax Enterprise Python Graph Extension
==========================================
This is the documentation for the DataStax Enterprise Python Graph Extension. This
extension is built on top of the `DataStax Enterprise Python driver <http://docs.datastax.com/en/developer/python-driver-dse>`_
and adds graph features, like executing TinkerPop traversals on DSE. Here are the main features of the extension:

* A TinkerPop GraphTraversalSource builder to execute traversals on DSE
* The ability to execution traversal queries explicitly, like other DSE requests work
* GraphSON serializers for DSE Graph types.

Contents
--------
:doc:`installation`
    How to install the extension.

:doc:`getting_started`
    First steps of connecting to DSE and executing traversal queries.

:doc:`api/index`
    The API documentation.

.. toctree::
   :hidden:

   api/index
   installation
   getting_started

Feedback/Issues
---------------
Please report any bugs and make any feature requests on the `JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_
issue tracker. You may also engage `DataStax support <https://support.datastax.com>`_ with any issues or feedback.
