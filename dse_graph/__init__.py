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
from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Traverser, TraversalSideEffects

from dse.cluster import Session, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT

from dse_graph.graphson import GraphSONReader, GraphSONWriter
from dse_graph._version import __version__, __version_info__


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger('dse_graph').addHandler(NullHandler())
log = logging.getLogger(__name__)


def graph_traversal_row_factory(column_names, rows):
    """
    Row Factory that returns the decoded graphson.
    """
    return [GraphSONReader.readObject(row[0])['result'] for row in rows]


def graph_traversal_traverser_row_factory(column_names, rows):
    """
    Row Factory that returns the decoded graphson as Traversers.
    """
    return [Traverser(GraphSONReader.readObject(row[0])['result']) for row in rows]


def _get_traversal_execution_profile(session, execution_profile, graph_name, row_factory=graph_traversal_row_factory):
    ep = session.execution_profile_clone_update(execution_profile, row_factory=row_factory)
    graph_options = ep.graph_options.copy()
    graph_options.graph_language='bytecode-json'
    graph_options.graph_name = graph_name
    ep.graph_options = graph_options
    return ep


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

    def __init__(self, session, graph_name, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        super(DSESessionRemoteGraphConnection, self).__init__(None, None)

        if not isinstance(session, Session):
            raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

        self.session = session
        self.graph_name = graph_name
        self.execution_profile = execution_profile

    def submit(self, bytecode):

        query = DseGraph.prepare_traversal_query(bytecode)

        execution_profile = _get_traversal_execution_profile(
            self.session, self.execution_profile, self.graph_name, row_factory=graph_traversal_traverser_row_factory)

        traversers = self.session.execute_graph(query, execution_profile=execution_profile)
        return RemoteTraversal(iter(traversers), TraversalSideEffects())

    def __str__(self):
        return "<DSESessionRemoteGraphConnection: graph_name='{0}'>".format(self.graph_name)
    __repr__ = __str__


class DseGraph(object):
    """
    Dse Graph utility class for GraphTraversal construction and execution.
    """

    @staticmethod
    def prepare_traversal_query(traversal):
        """
        Prepare and return a query string (GraphSON) generated with a traversal.

        :param traversal: The GraphTraversal object
        """

        try:
            query = GraphSONWriter.writeObject(traversal)
        except Exception as e:
            log.exception("Error preparing graphson traversal query:")
            raise

        return query

    @staticmethod
    def traversal_source(session=None, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        """
        Returns a TinkerPop GraphTraversalSource binded to the session and graph_name if provided.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name
        :param execution_profile: (Optional) Execution profile name for traversal queries.

        .. code-block:: python

            from dse.cluster import Cluster
            from dse_graph import DseGraph

            c = Cluster()
            session = c.connect()

            g = DseGraph.traversal_source(session, 'my_graph')
            print g.V().valueMap().toList():

        """

        graph = Graph()
        traversal_source = graph.traversal()

        if session:
            traversal_source = traversal_source.withRemote(
                DSESessionRemoteGraphConnection(session, graph_name, execution_profile))

        return traversal_source

    def execute_traversal(self, traversal, trace=False, execution_profile=None):
        """
        Execute a TinkerPop GraphTraversal synchronously.

        :param traversal: A TinkerPop GraphTraversal
        """
        return self.execute_traversal_async(traversal, trace=trace, execution_profile=execution_profile).result()

    def execute_traversal_async(self, traversal, trace=False, execution_profile=None):
        """
        Execute a TinkerPop GraphTraversal asynchronously and return a `ResponseFuture <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.ResponseFuture>`_
        object which callbacks may be attached to for asynchronous response delivery. You may also call ``ResponseFuture.result()`` to synchronously block for
        results at any time.

        :param traversal: A TinkerPop GraphTraversal
        """

        if not isinstance(traversal, GraphTraversal):
            raise TypeError('traversal must be an instance of GraphTraversal.')

        query = self.prepare_traversal_query(traversal)

        ep = execution_profile or self.execution_profile
        ep = _get_traversal_execution_profile(self.session, ep, self.graph_name)

        return self.session.execute_graph_async(query, trace=trace, execution_profile=ep)

    def __str__(self):
        return "<DseGraph: graph_name='{0}'>".format(self.graph_name)
    __repr__ = __str__
