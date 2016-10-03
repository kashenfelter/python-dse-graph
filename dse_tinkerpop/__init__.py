# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import logging

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal, RemoteTraversalSideEffects
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Traverser

from cassandra import ConsistencyLevel
from cassandra.cluster import default_lbp_factory

from dse.cluster import GraphExecutionProfile
from dse.policies import DSELoadBalancingPolicy
from dse.graph import GraphOptions

from dse_tinkerpop.graphson import GraphSONReader, GraphSONWriter


class NullHandler(logging.Handler):
    def emit(self, record):
            pass

logging.getLogger('dse_tinkerpop').addHandler(NullHandler())
log = logging.getLogger(__name__)

__version_info__ = (1, 0, '0a1')
__version__ = '.'.join(map(str, __version_info__))


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
    :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.
    """

    session = None
    execution_profile = None

    _execution_profile_key = None
    _EMPTY_SIDE_EFFECTS_KEYS = lambda: set()
    _EMPTY_SIDE_EFFECTS_VALUES = lambda k: []

    def __init__(self, session, graph_name=None, execution_profile=None):
        super(DSESessionRemoteGraphConnection, self).__init__(None, None)

        if not session:
            raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

        self.session = session
        self.execution_profile = execution_profile
        self._execution_profile_key = object()

        if not self.execution_profile:
            lbp = DSELoadBalancingPolicy(default_lbp_factory())
            self.execution_profile = GraphTraversalExecutionProfile(load_balancing_policy=lbp, request_timeout=60. * 3.)

        self.execution_profile.graph_options.graph_name = graph_name
        self.session.cluster.add_execution_profile(
            self._execution_profile_key,
            self.execution_profile
        )

    def submit(self, bytecode):

        try:
            query = GraphSONWriter.writeObject(bytecode)
        except Exception as e:
            log.exception("Error preparing graphson query:")
            raise

        traversers = self.session.execute_graph(query, execution_profile=self._execution_profile_key)
        return RemoteTraversal(iter(traversers),
                               RemoteTraversalSideEffects(self._EMPTY_SIDE_EFFECTS_KEYS, self._EMPTY_SIDE_EFFECTS_VALUES))


class DSETinkerPop(object):
    """
    DSE TinkerPop utility class for GraphTraversal construction and execution.
    """

    @staticmethod
    def graph_traversal(session, graph_name=None, execution_profile=None):
        """
        Returns a TinkerPop GraphTraversalSource to execute graph traversals on a DSE session.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name
        :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.

        .. code-block:: python

            from dse.cluster import Cluster
            from dse_tinkerpop import DSETinkerPop

            c = Cluster()
            session = c.connect()

            g = DSETinkerPop.graph_traversal(session, 'my_graph')
            print g.V().valueMap().toList():
        """
        graph = Graph()
        return graph.traversal().withRemote(DSESessionRemoteGraphConnection(session, graph_name, execution_profile))
