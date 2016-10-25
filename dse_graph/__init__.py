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
from gremlin_python.process.traversal import Traverser, TraversalSideEffects

from dse.cluster import Session, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
from dse.graph import GraphOptions

from dse_graph.graphson import GraphSONReader, GraphSONWriter, DseGraphSONReader
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


def graph_traversal_dse_object_row_factory(column_names, rows):
    """
    Row Factory that returns the decoded graphson as DSE types.
    """
    return [DseGraphSONReader.readObject(row[0])['result'] for row in rows]


class DSESessionRemoteGraphConnection(RemoteConnection):
    """
    A Tinkerpop RemoteConnection to execute traversal queries on DSE.

    :param session: A DSE session
    :param graph_name: (Optional) DSE Graph name.
    :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.
    """

    session = None
    graph_name = None
    execution_profile = None

    def __init__(self, session, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        super(DSESessionRemoteGraphConnection, self).__init__(None, None)

        if not isinstance(session, Session):
            raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

        self.session = session
        self.graph_name = graph_name
        self.execution_profile = execution_profile

    def submit(self, bytecode):

        query = DseGraph.query_from_traversal(bytecode)
        ep = self.session.execution_profile_clone_update(self.execution_profile, row_factory=graph_traversal_row_factory)

        if self.graph_name:
            graph_options = ep.graph_options.copy()
            graph_options.graph_name = self.graph_name
            ep.graph_options = graph_options

        traversers = self.session.execute_graph(query, execution_profile=ep)
        traversers = [Traverser(t) for t in traversers]
        return RemoteTraversal(iter(traversers), TraversalSideEffects())

    def __str__(self):
        return "<DSESessionRemoteGraphConnection: graph_name='{0}'>".format(self.graph_name)
    __repr__ = __str__


class DseGraph(object):
    """
    Dse Graph utility class for GraphTraversal construction and execution.
    """

    DSE_GRAPH_QUERY_LANGUAGE = 'bytecode-json'
    """
    Graph query language, Default is 'bytecode-json' (GraphSON).
    """

    @staticmethod
    def query_from_traversal(traversal):
        """
        From a GraphTraversal, return a query string based on the language specified in `DseGraph.DSE_GRAPH_QUERY_LANGUAGE`.

        :param traversal: The GraphTraversal object
        """

        language = DseGraph.DSE_GRAPH_QUERY_LANGUAGE
        if language == 'bytecode-json':
            try:
                query = GraphSONWriter.writeObject(traversal)
            except Exception as e:
                log.exception("Error preparing graphson traversal query:")
                raise
        else:
            raise NotImplementedError("Query language '{0}' is not implemented".format(DseGraph.DSE_GRAPH_QUERY_LANGUAGE))

        return query

    @staticmethod
    def traversal_source(session=None, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        """
        Returns a TinkerPop GraphTraversalSource binded to the session and graph_name if provided.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name
        :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to :class:`.GraphTraversalExecutionProfile`.

        .. code-block:: python

            from dse.cluster import Cluster
            from dse_graph import DseGraph

            c = Cluster()
            session = c.connect()

            g = DseGraph.traversal_source(session, 'my_graph')
            print g.V().valueMap().toList()

        """

        graph = Graph()
        traversal_source = graph.traversal()

        if session:
            traversal_source = traversal_source.withRemote(
                DSESessionRemoteGraphConnection(session, graph_name, execution_profile))

        return traversal_source

    @staticmethod
    def create_execution_profile(graph_name):
        """
        Creates an ExecutionProfile for GraphTraversal execution. You need to register that execution profile to the
        cluster by using `cluster.add_execution_profile`.

        :param graph_name: The graph name
        """

        ep = GraphExecutionProfile(row_factory=graph_traversal_dse_object_row_factory,
                                   graph_options=GraphOptions(graph_name=graph_name,
                                                              graph_language=DseGraph.DSE_GRAPH_QUERY_LANGUAGE))
        return ep
