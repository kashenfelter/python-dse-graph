# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import re
import os
import time
import requests
from os.path import expanduser

from ccmlib import common

from dse.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
from integration import PROTOCOL_VERSION, get_server_versions, BasicKeyspaceUnitTestCase, drop_keyspace_shutdown_cluster, get_cluster, get_node, teardown_package as base_teardown
from integration import use_singledc, use_single_node, wait_for_node_socket
from cassandra.protocol import ServerError

home = expanduser('~')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)
MAKE_STRICT = "schema.config().option('graph.schema_mode').set('production')"
MAKE_NON_STRICT = "schema.config().option('graph.schema_mode').set('development')"
ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true')"


def find_spark_master(session):

    # Itterate over the nodes the one with port 7080 open is the spark master
    for host in session.hosts:
        ip = host.address
        port = 7077
        spark_master = (ip, port)
        if common.check_socket_listening(spark_master, timeout=3):
            return spark_master[0]
    return None


def wait_for_spark_workers(num_of_expected_workers, timeout):
    """
    This queries the spark master and checks for the expected number of workers
    """
    start_time = time.time()
    match = False
    while True:
        r = requests.get("http://localhost:7080")
        match = re.search('Alive Workers:.*(\d+)</li>', r.text)
        num_workers = int(match.group(1))
        if num_workers == num_of_expected_workers:
            match = True
            break
        elif time.time() - start_time > timeout:
            match = True
            break
        time.sleep(1)
    return match


def teardown_package():
    base_teardown()


def use_single_node_with_graph(start=True):
    use_single_node(start=start, workloads=['graph'])


def use_single_node_with_graph_and_spark(start=True):
    use_single_node(start=start, workloads=['graph', 'spark'])


def use_singledc_wth_graph(start=True):
    use_singledc(start=start, workloads=['graph'])


def use_singledc_wth_graph_and_spark(start=True):
    use_cluster_with_graph(3)


def use_cluster_with_graph(num_nodes):
    """
    This is a  work around to account for the fact that spark nodes will conflict over master assignment
    when started all at once.
    """

    # Create the cluster but don't start it.
    use_singledc(start=False, workloads=['graph', 'spark'])
    # Start first node.
    get_node(1).start(wait_for_binary_proto=True)
    # Wait binary protocol port to open
    wait_for_node_socket(get_node(1), 120)
    # Wait for spark master to start up
    spark_master_http = ("localhost", 7080)
    common.check_socket_listening(spark_master_http, timeout=60)
    tmp_cluster = Cluster(protocol_version=PROTOCOL_VERSION)

    # Start up remaining nodes.
    try:
        session = tmp_cluster.connect()
        statement = "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '%d'}" % (num_nodes)
        session.execute(statement)
    finally:
        tmp_cluster.shutdown()

    for i in range(1, num_nodes+1):
        if i is not 1:
            node = get_node(i)
            node.start(wait_for_binary_proto=True)
            wait_for_node_socket(node, 120)

    # Wait for workers to show up as Alive on master
    wait_for_spark_workers(3, 120)


def reset_graph(session, graph_name):
        session.execute_graph('system.graph(name).ifNotExists().create()', {'name': graph_name})
        wait_for_graph_inserted(session, graph_name)


def wait_for_graph_inserted(session, graph_name):
        count = 0
        exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name})[0].value
        while not exists and count < 50:
            time.sleep(1)
            exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name})[0].value
        return exists


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """
    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.session_setup()
        self.reset_graph()
        profiles = self.cluster.profile_manager.profiles
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = self.graph_name
        profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].graph_options.graph_name = self.graph_name
        self.clear_schema()

    def tearDown(self):
        self.cluster.shutdown()

    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)


class BasicGeometricUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This base test class is used by all the geomteric tests. It contains class level teardown and setup
    methods. It also contains the test fixtures used by those tests
    """
    @classmethod
    def common_dse_setup(cls, rf, keyspace_creation=True):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        if keyspace_creation:
            cls.create_keyspace(rf)
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.session.set_keyspace(cls.ks_name)

    @classmethod
    def setUpClass(cls):
        cls.common_dse_setup(1)
        cls.initalizeTables()

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)

    @classmethod
    def initalizeTables(cls):
        udt_type = "CREATE TYPE udt1 (g {0})".format(cls.cql_type_name)
        large_table = "CREATE TABLE tbl (k uuid PRIMARY KEY, g {0}, l list<{0}>, s set<{0}>, m0 map<{0},int>, m1 map<int,{0}>, t tuple<{0},{0},{0}>, u frozen<udt1>)".format(cls.cql_type_name)
        simple_table = "CREATE TABLE tblpk (k {0} primary key, v int)".format( cls.cql_type_name)
        cluster_table = "CREATE TABLE tblclustering (k0 int, k1 {0}, v int, primary key (k0, k1))".format(cls.cql_type_name)
        cls.session.execute(udt_type)
        cls.session.execute(large_table)
        cls.session.execute(simple_table)
        cls.session.execute(cluster_table)


def generate_classic(session):
    to_run = [MAKE_STRICT, ALLOW_SCANS, '''schema.propertyKey('name').Text().ifNotExists().create();
            schema.propertyKey('age').Int().ifNotExists().create();
            schema.propertyKey('lang').Text().ifNotExists().create();
            schema.propertyKey('weight').Float().ifNotExists().create();
            schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
            schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
            schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
            schema.edgeLabel('created').connection('software', 'software').add();
            schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();''',
            '''Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
            Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
            Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
            Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
            Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
            Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
            marko.addEdge('knows', vadas, 'weight', 0.5f);
            marko.addEdge('knows', josh, 'weight', 1.0f);
            marko.addEdge('created', lop, 'weight', 0.4f);
            josh.addEdge('created', ripple, 'weight', 1.0f);
            josh.addEdge('created', lop, 'weight', 0.4f);
            peter.addEdge('created', lop, 'weight', 0.2f);''']
    for run in to_run:
        succeed = False
        count = 0
        # Retry up to 10 times this is an issue for
        # Graph Mult-NodeClusters
        while count < 10 and not succeed:
            try:
                session.execute_graph(run)
                succeed = True
            except (ServerError):
                print("error creating classic graph retrying")
                time.sleep(.5)
            count += 1


