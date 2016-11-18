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
    import unittest

from dse_graph import DseGraph
import time
from dse_graph.predicates import Search, Geo
from dsetest.integration import BasicSharedGraphUnitTestCase, use_single_node_with_graph_and_solr, generate_address_book_graph
from dse.util import Distance, Polygon



def setup_module():
    use_single_node_with_graph_and_solr()


class AbstractSearchTest():


    def test_search_by_prefix(self):

        g = self.fetch_traversal_source()
        traversal = g.V().has("person", "name", Search.prefix("Paul")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 1)
        self.assertEqual(results_list[0], "Paul Thomas Joe")


    def test_search_by_regex(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "name", Search.regex(".*Paul.*")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn("James Paul Smith", results_list )

    def test_search_by_token(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token("cold")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Jill Alice", results_list )
        self.assertIn("George Bill Steve", results_list)


    def test_search_by_token_prefix(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token_prefix("h")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "James Paul Smith", results_list )


    def test_search_by_token_regex(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token_regex("(nice|hospital)")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "Jill Alice", results_list )

    def test_search_by_distance(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "coordinates", Geo.inside(Distance(-92, 44, 2))).values("name");
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "George Bill Steve", results_list )

    @unittest.skip
    def test_search_by_polygon_area(self):

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "coordinates", Geo.inside(Polygon([(-85, 40), (-92.5, 45), (-95, 38), (-85, 40)])))
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "James Paul Smith", results_list )


class ImplicitSearchTest(AbstractSearchTest, BasicSharedGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using implicit execution
    This all traversal will be run directly using toList()
    """
    @classmethod
    def setUpClass(self):
        super(ImplicitSearchTest, self).setUpClass()
        self.ep = DseGraph().create_execution_profile(self.graph_name)
        self.cluster.add_execution_profile(self.graph_name, self.ep)
        generate_address_book_graph(self.session, 0)
        time.sleep(20)

    def fetch_key_from_prop(self, property):
        return property.key

    def fetch_traversal_source(self):
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=self.ep)

    def execute_traversal(self, traversal):
        return traversal.toList()


class ExplicitSearchTest(AbstractSearchTest, BasicSharedGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using implicit execution
    This all traversal will be run directly using toList()
    """
    @classmethod
    def setUpClass(self):
        super(ExplicitSearchTest, self).setUpClass()
        self.ep = DseGraph().create_execution_profile(self.graph_name)
        self.cluster.add_execution_profile(self.graph_name, self.ep)
        generate_address_book_graph(self.session, 0)
        time.sleep(20)

    def fetch_traversal_source(self):
        return DseGraph().traversal_source(self.session, self.graph_name)

    def execute_traversal(self, traversal):
        query = DseGraph.query_from_traversal(traversal)
        import pdb;pdb.set_trace()
        #Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.session.execute_graph(query, execution_profile=self.ep)
        return list(result_set)

