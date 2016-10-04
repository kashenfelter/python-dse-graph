# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import logging
import copy

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal, RemoteTraversalSideEffects
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Traverser

from cassandra import ConsistencyLevel

from dse.cluster import Session, GraphExecutionProfile
from dse.graph import GraphOptions

from dse_tinkerpop.graphson import GraphSONReader, GraphSONWriter


EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT = object()
"""
Key for the default graph traversal execution profile, used when no other profile is selected in
``DSETinkerPop.execute_traversal()``.

Use this as the key in `Cluster(execution_profiles) <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_
to override the default graph profile.
"""


class NullHandler(logging.Handler):
    def emit(self, record):
            pass

logging.getLogger('dse_tinkerpop').addHandler(NullHandler())
log = logging.getLogger(__name__)

__version_info__ = (1, 0, '0a1')
__version__ = '.'.join(map(str, __version_info__))


def _register_traversal_execution_profile(session):
    if EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT not in session.cluster.profile_manager.profiles:
        session.cluster.add_execution_profile(
            EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT,
            GraphTraversalExecutionProfile()
        )


def graph_traversal_row_factory(column_names, rows):
    """
    Row Factory that returns the decoded graphson as Traversers.
    """
    return [Traverser(GraphSONReader.readObject(row[0])['result']) for row in rows]


class GraphTraversalExecutionProfile(GraphExecutionProfile):
    graph_options = None
    """
    DSE GraphOptions to use with this execution

    Default options for graph traversal queries, initialized as follows by default::

        GraphOptions(graph_source='g',
                     graph_language='bytecode-json')

    See dse.graph.GraphOptions
    """

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=30.0, row_factory=graph_traversal_row_factory,
                 graph_options=None):
        """
        Default execution profile for graph traversal execution.

        See also :class:`~.GraphExecutionPolicy`.
        """
        super(GraphTraversalExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                            serial_consistency_level, request_timeout, row_factory)
        self.graph_options = graph_options or GraphOptions(graph_source='g',
                                                           graph_language='bytecode-json')


class DSESessionRemoteGraphConnection(RemoteConnection):
    """
    A TinkerPop RemoteConnection to execute traversal queries on DSE.

    :param session: A DSE session
    :param graph_name: (Optional) DSE Graph name
    :param execution_profile: (Optional) Execution profile name for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.
    """

    session = None
    graph_name = None
    execution_profile = None

    _EMPTY_SIDE_EFFECTS_KEYS = lambda: set()
    _EMPTY_SIDE_EFFECTS_VALUES = lambda k: []

    def __init__(self, session, graph_name=None, execution_profile=None):
        super(DSESessionRemoteGraphConnection, self).__init__(None, None)

        if not isinstance(session, Session):
            raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

        self.session = session
        self.graph_name = graph_name
        self.execution_profile = execution_profile
        _register_traversal_execution_profile(session)

    def submit(self, bytecode):

        try:
            query = GraphSONWriter.writeObject(bytecode)
        except Exception as e:
            log.exception("Error preparing graphson traversal query:")
            raise

        ep = self.execution_profile or EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT
        ep_tmp = self.session.execution_profile_clone_update(ep)
        graph_options = copy.copy(ep_tmp.graph_options)
        graph_options.graph_name = self.graph_name

        traversers = self.session.execute_graph(query, execution_profile=ep_tmp)
        return RemoteTraversal(iter(traversers),
                               RemoteTraversalSideEffects(self._EMPTY_SIDE_EFFECTS_KEYS, self._EMPTY_SIDE_EFFECTS_VALUES))


class DSETinkerPop(object):
    """
    DSE TinkerPop utility class for GraphTraversal construction and execution.

    :param session: A DSE session
    :param graph_name: (Optional) DSE Graph name
    :param execution_profile: (Optional) Execution profile name for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.
    """

    session = None
    execution_profile = None
    graph_name = None

    def __init__(self, session, graph_name=None, execution_profile=None):

        if not isinstance(session, Session):
            raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

        self.session = session
        self.graph_name = graph_name
        self.execution_profile = execution_profile
        _register_traversal_execution_profile(session)

    @staticmethod
    def graph_traversal_source_from_session(session, graph_name=None, execution_profile=None):
        """
        Returns a TinkerPop GraphTraversalSource to execute graph traversals on a DSE session.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name
        :param execution_profile: (Optional) Execution profile name for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.

        .. code-block:: python

            from dse.cluster import Cluster
            from dse_tinkerpop import DSETinkerPop

            c = Cluster()
            session = c.connect()

            g = DSETinkerPop.graph_traversal_source_from_session(session, 'my_graph')
            print g.V().valueMap().toList():
        """
        graph = Graph()
        return graph.traversal().withRemote(DSESessionRemoteGraphConnection(session, graph_name, execution_profile))

    def graph_traversal_source(self):
        """
        Returns a TinkerPop GraphTraversalSource binded to the DSETinkerPop instance session.

        .. code-block:: python

            from dse.cluster import Cluster
            from dse_tinkerpop import DSETinkerPop

            c = Cluster()
            session = c.connect()

            dse_tinkerpop = DSETinkerPop(session, 'my_graph')
            g = dse_tinkerpop.graph_traversal_source()
            print g.V().valueMap().toList():

        """
        graph = Graph()
        return graph.traversal().withRemote(
            DSESessionRemoteGraphConnection(self.session, self.graph_name, self.execution_profile))

    def execute_traversal(self, traversal, trace=False, execution_profile=None):
        """
        Execute a TinkerPop GraphTraversal synchronously.

        :param traversal: A TinkerPop GraphTraversal
        """
        return self.execute_traversal_async(traversal, trace=trace, execution_profile=execution_profile).result()

    def execute_traversal_async(self, traversal, trace=False, execution_profile=None):
        """
        Execute a TinkerPop GraphTraversal asynchronously and return a `ResponseFuture <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.ResponseFuture.result>`_
        object which callbacks may be attached to for asynchronous response delivery. You may also call ``ResponseFuture.result()`` to synchronously block for
        results at any time.
        """

        if not isinstance(traversal, GraphTraversal):
            raise TypeError('traversal must be an instance of GraphTraversal.')

        try:
            query = GraphSONWriter.writeObject(traversal)
        except Exception:
            log.exception("Error preparing graphson traversal query:")
            raise

        ep = execution_profile or self.execution_profile or EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT
        ep_tmp = self.session.execution_profile_clone_update(ep)
        graph_options = copy.copy(ep_tmp.graph_options)
        graph_options.graph_name = self.graph_name

        return self.session.execute_graph_async(query, trace=trace, execution_profile=ep_tmp)
