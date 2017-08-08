__author__ = "Jeremy Nelson"

import os
import rdflib
import sys
import unittest

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

TEST_HOME = os.path.abspath(os.path.dirname(__file__))
BASE_HOME = os.path.abspath(os.path.dirname(TEST_HOME))
sys.path.append(BASE_HOME)
import utilities

class TestDateGenerator(unittest.TestCase):

    def setUp(self):
        work = rdflib.URIRef("https://plains2peaks.org/1234#Work")
        graph = rdflib.Graph()
        graph.namespace_manager.bind("bf", BF)
        graph.add((work, rdflib.RDF.type, BF.Work))
        self.generator = utilities.DateGenerator(graph=graph)

    def test_init_error_graph(self):
        with self.assertRaises(AttributeError):
            date_generator = utilities.DateGenerator() 


    def test_init_error_missing_work(self):
        with self.assertRaises(ValueError):
            date_generator = utilities.DateGenerator(graph=rdflib.Graph())
        

    def test_date_range_all_years(self):
        self.assertEquals(len(self.generator.graph), 1)
        raw_range = "1940-1950"
        self.generator.run(raw_range)
        self.assertEquals(len(self.generator.graph), 35)
        
    def test_multiple_date_ranges(self):
        self.assertEquals(len(self.generator.graph), 1)
        self.generator.run("1995-1997, 1820-1839")
        self.assertEquals(len(self.generator.graph), 72)


    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
