# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import sys
from dse_graph import DseGraph
from tests.integration.advanced import BasicGraphUnitTestCase, use_single_node_with_graph_and_solr, generate_classic, generate_line_graph, generate_multi_field_graph, generate_large_complex_graph, generate_type_graph_schema, \
    validate_classic_vertex, validate_classic_edge, validate_generic_vertex_result_type, validate_classic_edge_properties, validate_line_edge, validate_generic_edge_result_type, validate_path_result_type, TYPE_MAP
from gremlin_python.structure.graph import Edge as TravEdge
from gremlin_python.structure.graph import Vertex as TravVertex
from dse.util import Point, Polygon, LineString
import datetime
from six import string_types


def setup_module():
    use_single_node_with_graph_and_solr()


class AbstractTraversalTest():

    def test_basic_query(self):
        """
        Test to validate that basic graph queries works

        Creates a simple classic tinkerpot graph, and attempts to preform a basic query
        using Tinkerpop's GLV with both explicit and implicit execution
        ensuring that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """


        g = self.fetch_traversal_source()
        generate_classic(self.session)
        traversal =g.V().has('name', 'marko').out('knows').values('name')
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def test_classic_graph(self):
        """
        Test to validate that basic graph generation, and vertex and edges are surfaced correctly

        Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
        using Tinkerpop's GLV with both explicit and implicit execution
        ensuring that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """

        generate_classic(self.session)
        g = self.fetch_traversal_source()
        traversal =  g.V()
        vert_list = self.execute_traversal(traversal)

        for vertex in vert_list:
            self._validate_classic_vertex(g, vertex)
        traversal =  g.E()
        edge_list = self.execute_traversal(traversal)
        for edge in edge_list:
            self._validate_classic_edge(g, edge)

    def test_graph_classic_path(self):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result path object should be unpacked correctly including all nested edges and verticies
        @test_category dse graph
        """
        generate_classic(self.session)
        g = self.fetch_traversal_source()
        traversal = g.V().hasLabel('person').has('name', 'marko').as_('a').outE('knows').inV().as_('c', 'd').outE('created').as_('e', 'f', 'g').inV().path()
        path_list = self.execute_traversal(traversal)
        self.assertEqual(len(path_list), 2)
        for path in path_list:
            self._validate_path_result_type(g, path)

    def test_range_query(self):
        """
        Test to validate range queries are handled correctly.

        Creates a very large line graph script and executes it. Then proceeds to to a range
        limited query against it, and ensure that the results are formated correctly and that
        the result set is properly sized.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result result set should be properly formated and properly sized

        @test_category dse graph
        """


        query_to_run = generate_line_graph(250)
        self.session.execute_graph(query_to_run)
        g = self.fetch_traversal_source()

        traversal = g.E().range(0,10)
        edges = self.execute_traversal(traversal)
        self.assertEqual(len(edges), 10)
        for edge in edges:
            self._validate_line_edge(g, edge)

    def test_result_types(self):
        """
        Test to validate that the edge and vertex version of results are constructed ccorrectly.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        generate_multi_field_graph(self.session)  # TODO: we could just make a single vertex with properties of all types, or even a simple query that just uses a sequence of groovy expressions
        g = self.fetch_traversal_source()
        traversal = g.V()
        vertices = self.execute_traversal(traversal)
        for vertex in vertices:
            self._validate_type(g, vertex)

    def test_large_result_set(self):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        generate_large_complex_graph(self.session, 5000)
        g = self.fetch_traversal_source()
        traversal = g.V()
        vertices = self.execute_traversal(traversal)
        for vertex in vertices:
            self._validate_generic_vertex_result_type(g,vertex)

    def test_vertex_meta_properties(self):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        s = self.session
        s.execute_graph("schema.propertyKey('k0').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('k1').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('key').Text().properties('k0', 'k1').ifNotExists().create();")
        s.execute_graph("schema.vertexLabel('MLP').properties('key').ifNotExists().create();")
        s.execute_graph("schema.config().option('graph.allow_scan').set('true');")
        v = s.execute_graph('''v = graph.addVertex('MLP')
                                 v.property('key', 'meta_prop', 'k0', 'v0', 'k1', 'v1')
                                 v''')[0]

        g = self.fetch_traversal_source()

        traversal = g.V()
        # This should contain key, and value where value is a property
        # This should be a vertex property and should contain sub properties
        results = self.execute_traversal(traversal)
        self._validate_meta_property(g, results[0])

    def test_vertex_multiple_properties(self):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        s = self.session
        s.execute_graph('''Schema schema = graph.schema();
                           schema.propertyKey('mult_key').Text().multiple().ifNotExists().create();
                           schema.propertyKey('single_key').Text().single().ifNotExists().create();
                           schema.vertexLabel('MPW1').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('MPW2').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('SW1').properties('single_key').ifNotExists().create();''')

        mpw1v = s.execute_graph('''v = graph.addVertex('MPW1')
                                 v.property('mult_key', 'value')
                                 v''')[0]

        mpw2v = s.execute_graph('''g.addV(label, 'MPW2', 'mult_key', 'value0', 'mult_key', 'value1')''')[0]

        g = self.fetch_traversal_source()
        traversal = g.V(mpw1v.id).properties()

        vertex_props = self.execute_traversal(traversal)

        self.assertEqual(len(vertex_props), 1)

        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "mult_key")
        self.assertEqual(vertex_props[0].value, "value")

        # multiple_with_two_values
         #v = s.execute_graph('''g.addV(label, 'MPW2', 'mult_key', 'value0', 'mult_key', 'value1')''')[0]
        traversal = g.V(mpw2v.id).properties()

        vertex_props = self.execute_traversal(traversal)

        self.assertEqual(len(vertex_props), 2)
        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), 'mult_key')
        self.assertEqual(self.fetch_key_from_prop(vertex_props[1]), 'mult_key')
        self.assertEqual(vertex_props[0].value, 'value0')
        self.assertEqual(vertex_props[1].value, 'value1')

        # single_with_one_value
        v = s.execute_graph('''v = graph.addVertex('SW1')
                                 v.property('single_key', 'value')
                                 v''')[0]
        traversal = g.V(v.id).properties()
        vertex_props = self.execute_traversal(traversal)
        self.assertEqual(len(vertex_props), 1)
        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "single_key")
        self.assertEqual(vertex_props[0].value, "value")


    def should_parse_meta_properties(self):
        g = self.fetch_traversal_source()
        g.addV("meta_v").property("meta_prop", "hello", "sub_prop", "hi", "sub_prop2", "hi2")


    def test_all_graph_types_with_schema(self):
        """
        Exhaustively goes through each type that is supported by dse_graph.
        creates a vertex for each type  using a dse-tinkerpop traversal,
        It then attempts to fetch it from the server and compares it to what was inserted
        Prime the graph with the correct schema first

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result inserted objects are equivalent to those retrieved

        @test_category dse graph
        """
        generate_type_graph_schema(self.session)
        # if result set is not parsed correctly this will throw an exception

        g = self.fetch_traversal_source()
        for key in TYPE_MAP.keys():
            vertex_label = key
            property_name= key+"value"
            traversal = g.addV(vertex_label).property(property_name, TYPE_MAP[key][1])
            results = self.execute_traversal(traversal)


        traversal = g.V()
        vertices = self.execute_traversal(traversal)
        for vertex in vertices:
            original = TYPE_MAP[vertex.label][1]
            self._check_equality(g, original, vertex)

    def test_all_graph_types_without_schema(self):
        """
        Exhaustively goes through each type that is supported by dse_graph.
        creates a vertex for each type  using a dse-tinkerpop traversal,
        It then attempts to fetch it from the server and compares it to what was inserted
        Do not prime the graph with the correct schema first

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result inserted objects are equivalent to those retrieved

        @test_category dse graph
        """

        # Prime graph using common utilites
        generate_type_graph_schema(self.session, prime_schema=False)
        g = self.fetch_traversal_source()
        # For each supported type fetch create a vetex containing that type
        for key in TYPE_MAP.keys():
            vertex_label = key
            property_name= key+"value"
            traversal = g.addV(vertex_label).property(property_name, TYPE_MAP[key][1])
            self.execute_traversal(traversal)
        traversal = g.V()
        vertices = self.execute_traversal(traversal)

        # Iterate over all the vertices and check that they match the original input
        for vertex in vertices:
            original = TYPE_MAP[vertex.label][1]
            self._check_equality(g, original, vertex)


    def fetch_edge_props(self, g, edge):
        edge_props = g.E(edge.id).properties().toList()
        return edge_props

    def fetch_vertex_props(self, g, vertex):

        vertex_props = g.V(vertex.id).properties().toList()
        return vertex_props


class ImplicitExecutionTest(AbstractTraversalTest, BasicGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using implicit execution
    This all traversal will be run directly using toList()
    """
    def setUp(self):
        super(ImplicitExecutionTest, self).setUp()
        self.ep = DseGraph().create_execution_profile(self.graph_name)
        self.cluster.add_execution_profile(self.graph_name, self.ep)

    def fetch_key_from_prop(self, property):
        return property.key

    def fetch_traversal_source(self):
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=self.ep)

    def execute_traversal(self, traversal):
        return traversal.toList()

    def _validate_classic_vertex(self, g, vertex):
        # Checks the properties on a classic vertex for correctness
        vertex_props = self.fetch_vertex_props(g, vertex)
        vertex_prop_keys = [vp.key for vp in vertex_props]
        self.assertEqual(len(vertex_prop_keys), 2)
        self.assertIn('name', vertex_prop_keys)
        self.assertTrue('lang' in vertex_prop_keys or 'age' in vertex_prop_keys)

    def _validate_generic_vertex_result_type(self,g, vertex):
        # Checks a vertex object for it's generic properties
        properties = self.fetch_vertex_props(g, vertex)
        for attr in ('id', 'label'):
            self.assertIsNotNone(getattr(vertex, attr))
        self.assertTrue( len(properties)>2)

    def _validate_classic_edge_properties(self, g, edge):
        # Checks the properties on a classic edge for correctness
        edge_props = self.fetch_edge_props(g, edge)
        edge_prop_keys = [ep.key for ep in edge_props]
        self.assertEqual(len(edge_prop_keys), 1)
        self.assertIn('weight', edge_prop_keys)

    def _validate_classic_edge(self, g, edge):
        self._validate_generic_edge_result_type(edge)
        self._validate_classic_edge_properties(g, edge)

    def _validate_line_edge(self, g, edge):
        self._validate_generic_edge_result_type(edge)
        edge_props = self.fetch_edge_props(g, edge)
        edge_prop_keys = [ep.key for ep in edge_props]
        self.assertEqual(len(edge_prop_keys), 1)
        self.assertIn('distance', edge_prop_keys)

    def _validate_generic_edge_result_type(self, edge):
        self.assertIsInstance(edge, TravEdge)

        for attr in ('outV', 'inV', 'label', 'id'):
            self.assertIsNotNone(getattr(edge, attr))

    def _validate_path_result_type(self, g, objects_path):
        for obj in objects_path:
            if isinstance(obj, TravEdge):
                self._validate_classic_edge(g, obj)
            elif isinstance(obj, TravVertex):
                self._validate_classic_vertex(g, obj)
            else:
                self.fail("Invalid object found in path " + str(object.type))

    def _check_equality(self,g, original, vertex):
        prop = self.fetch_vertex_props(g, vertex)[0]
        if isinstance(original, float):
            self.assertAlmostEqual(original, prop.value, delta=.01)
        else:
            self.assertEqual(original, prop.value)


    def _validate_meta_property(self, g, vertex):
        meta_props =  g.V(vertex.id).properties().toList()
        self.assertEqual(len(meta_props), 1)
        meta_prop = meta_props[0]
        self.assertEqual(meta_prop.value,"meta_prop")
        self.assertEqual(meta_prop.key,"key")

        nested_props = vertex_props = g.V(vertex.id).properties().properties().toList()
        self.assertEqual(len(nested_props), 2)
        for nested_prop in nested_props:
            self.assertTrue(nested_prop.key in ['k0', 'k1'])
            self.assertTrue(nested_prop.value in ['v0', 'v1'])

    def _validate_type(self, g,  vertex):
        props = self.fetch_vertex_props(g, vertex)
        for prop in props:
            value =  prop.value
            if any(prop.key.startswith(t) for t in ('int', 'short')):
                typ = int

            elif any(prop.key.startswith(t) for t in ('long',)):
                if sys.version_info >= (3, 0):
                    typ = int
                else:
                    typ = long
            elif any(prop.key.startswith(t) for t in ('float', 'double')):
                typ = float
            elif any(prop.key.startswith(t) for t in ('polygon',)):
                typ = Polygon
            elif any(prop.key.startswith(t) for t in ('point',)):
                typ = Point
            elif any(prop.key.startswith(t) for t in ('Linestring',)):
                typ = LineString
            elif any(prop.key.startswith(t) for t in ('neg',)):
                typ=string_types
            elif any(prop.key.startswith(t) for t in ('date',)):
                typ = datetime.date
            else:
                self.fail("Received unexpected type: %s" % prop.key)
            self.assertIsInstance(value, typ)


class ExplicitExecutionTest(AbstractTraversalTest, BasicGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using Explicit execution
    All queries will be run by converting them to byte code, and calling execute graph explicitly with a generated ep.
    """
    def setUp(self):
        super(ExplicitExecutionTest, self).setUp()
        self.ep = DseGraph().create_execution_profile(self.graph_name)
        self.cluster.add_execution_profile(self.graph_name, self.ep)

    def fetch_key_from_prop(self, property):
        return property.label

    def fetch_traversal_source(self):
        return DseGraph().traversal_source(self.session, self.graph_name)

    def execute_traversal(self, traversal):
        query = DseGraph.query_from_traversal(traversal)
        #Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.session.execute_graph(query, execution_profile=self.ep)
        return list(result_set)

    def _validate_classic_vertex(self, g, vertex):
        validate_classic_vertex(self, vertex)

    def _validate_generic_vertex_result_type(self,g, vertex):
        validate_generic_vertex_result_type(self, vertex)

    def _validate_classic_edge_properties(self, g, edge):
        validate_classic_edge_properties(self, edge)

    def _validate_classic_edge(self, g, edge):
        validate_classic_edge

    def _validate_line_edge(self, g, edge):
        validate_line_edge(self, edge)

    def _validate_generic_edge_result_type(self, edge):
        validate_generic_edge_result_type(self, edge)

    def _validate_type(self, g,  vertex):
        for key in vertex.properties:
            value =  vertex.properties[key][0].value
            if any(key.startswith(t) for t in ('int', 'short')):
                typ = int
            elif any(key.startswith(t) for t in ('long',)):
                if sys.version_info >= (3, 0):
                    typ = int
                else:
                    typ = long
            elif any(key.startswith(t) for t in ('float', 'double')):
                typ = float
            elif any(key.startswith(t) for t in ('polygon',)):
                typ = Polygon
            elif any(key.startswith(t) for t in ('point',)):
                typ = Point
            elif any(key.startswith(t) for t in ('Linestring',)):
                typ = LineString
            elif any(key.startswith(t) for t in ('neg',)):
                typ=string_types
            elif any(key.startswith(t) for t in ('date',)):
                typ = datetime.date
            else:
                self.fail("Received unexpected type: %s" % key)
            self.assertIsInstance(value, typ)

    def _validate_path_result_type(self, g, path_obj):
        validate_path_result_type(self, path_obj)

    def _validate_meta_property(self, g, vertex):

        self.assertEqual(len(vertex.properties), 1)
        self.assertEqual(len(vertex.properties['key']), 1)
        p = vertex.properties['key'][0]
        self.assertEqual(p.label, 'key')
        self.assertEqual(p.value, 'meta_prop')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})

    def _check_equality(self,g, original, vertex):
        for key in vertex.properties:
            value = vertex.properties[key][0].value
            if isinstance(original, float):
                self.assertAlmostEqual(original, value, delta=.01)
            else:
                self.assertEqual(original, value)
