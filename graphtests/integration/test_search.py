# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from dse_graph import DseGraph
import time
from dse_graph.predicates import Search, Geo
from tests.integration.advanced import BasicSharedGraphUnitTestCase, generate_address_book_graph, use_single_node_with_graph_and_solr
from tests.integration import greaterthanorequaldse51
from dse.util import Distance


def setup_module():
    use_single_node_with_graph_and_solr()


class AbstractSearchTest():

    def test_search_by_prefix(self):
        """
        Test to validate that solr searches by prefix function.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names starting with Paul should be returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal = g.V().has("person", "name", Search.prefix("Paul")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 1)
        self.assertEqual(results_list[0], "Paul Thomas Joe")

    def test_search_by_regex(self):
        """
        Test to validate that solr searches by regex function.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names containing Paul should be returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "name", Search.regex(".*Paul.*")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn("James Paul Smith", results_list )

    def test_search_by_token(self):
        """
        Test to validate that solr searches by token.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing could shoud be returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token("cold")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Jill Alice", results_list )
        self.assertIn("George Bill Steve", results_list)


    def test_search_by_token_prefix(self):
        """
        Test to validate that solr searches by token prefix.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing a token starting with h are returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token_prefix("h")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "James Paul Smith", results_list )


    def test_search_by_token_regex(self):
        """
        Test to validate that solr searches by token regex.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing nice or hospital are returned

        @test_category dse graph
        """

        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token_regex("(nice|hospital)")).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "Jill Alice", results_list )

    def test_search_by_distance(self):
        """
        Test to validate that solr searches by distance.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with a geo location within a 2 radius distance of -92,44 are returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "coordinates", Geo.inside(Distance(-92, 44, 2))).values("name");
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "George Bill Steve", results_list )

    @greaterthanorequaldse51
    def test_search_by_fuzzy(self):
        """
        Test to validate that solr searches by distance.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with a geo location within a 2 radius distance of -92,44 are returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "name", Search.fuzzy("Paul Thamas Joe" ,1)).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 1)
        self.assertIn("Paul Thomas Joe", results_list )

        traversal =  g.V().has("person", "name", Search.fuzzy("Paul Thames Joe" ,1)).values("name")
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 0)

    @greaterthanorequaldse51
    def test_search_by_fuzzy_token(self):
        """
        Test to validate that fuzzy searches.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with that differ from the search criteria by one letter should be returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.token_fuzzy("lives", 1)).values("name");
        # Should match 'Paul Thomas Joe' since description contains 'Lives'
        # Should match 'James Paul Joe' since description contains 'Likes'
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn("James Paul Smith", results_list )

        traversal =  g.V().has("person", "description", Search.token_fuzzy("loues", 1)).values("name");
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 0)

    @greaterthanorequaldse51
    def test_search_by_phrase(self):
        """
        Test to validate that phrase searches.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with that differ from the search phrase criteria by two letter should be returned

        @test_category dse graph
        """
        g = self.fetch_traversal_source()
        traversal =  g.V().has("person", "description", Search.phrase("a cold", 2)).values("name");
        #Should match 'George Bill Steve' since 'A cold dude' is at distance of 0 for 'a cold'.
        #Should match 'Jill Alice' since 'Enjoys a very nice cold coca cola' is at distance of 2 for 'a cold'.
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn('George Bill Steve', results_list )
        self.assertIn('Jill Alice', results_list )

        traversal =  g.V().has("user", "description", Search.phrase("a bald", 2)).values("name");
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 0)



class ImplicitSearchTest(AbstractSearchTest, BasicSharedGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractSearchTest using implicit execution
    All traversals will be run directly using toList()
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
    This test class will execute all tests of the AbstractSearchTest using implicit execution
    All traversals will be converted to byte code then they will be executed explicitly.
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
        #Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.session.execute_graph(query, execution_profile=self.ep)
        return list(result_set)

