__author__ = "Jeremy Nelson"

import requests
TRIPLESTORE_URL = "http://localhost:9999/blazegraph/sparql"

def report_router(report_name):
    if report_name.startswith("rights-statements"):
        return rights_statements()
    
def rights_statements():
    output = []
    sparql = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT ?rights_statement (count(?item) as ?count) WHERE{
  ?item rdf:type bf:Item ;
  OPTIONAL { ?item bf:AccessPolicy ?rights_statement }
  OPTIONAL { ?item bf:usageAndAccessPolicy ?rights_statement }
  
} GROUP BY ?rights_statement"""
    result = requests.post(TRIPLESTORE_URL,
        data={"query": sparql,
              "format": "json"})
    bindings = result.json().get("results").get("bindings")
    for row in bindings:
        output.append({"url": row.get("rights_statement", 
                                      {'value': 'Unknown'}).get('value'),
                       "count": row.get("count").get('value')})
    return output
