__author__ = "Jeremy Nelson"

from collections import OrderedDict
import requests

INSTITUTIONS = OrderedDict([
    ('http://www.uwyo.edu/ahc/', 'American Heritage Center'),
    ('https://www.steamboatlibrary.org/',
     'Bud Werner Memorial Library'),
    ('https://www.coloradocollege.edu/', 'Colorado College'),
    ('https://www.cde.state.co.us/cdelib', 'Colorado State Library'),
    ('https://www.cde.state.co.us/stateinfo',
     'Colorado State Publications Library'),
    ('https://www.denverlibrary.org/', 'Denver Public Library'),
    ('https://www.evld.org/', 'Eagle Valley Library District'),
    ('https://library.fortlewis.edu/', 'Fort Lewis College'),
    ('https://www.gunnisoncountylibraries.org/',
     'Gunnison County Library District'),
    ('http://www.historycolorado.org/', 'History Colorado'),
    ('http://www.western.edu/academics/leslie-j-savage-library',
     'Leslie J. Savage Library'),
    ('https://marmot.org/', 'Marmot Library Network'),
    ('https://mesacountylibraries.org/', 'Mesa County Libraries'),
    ('http://prlibrary.org/', "Pine River Library"),
    ('http://www.salidalibrary.org/',
     'Salida Regional Library (Salida, Colo.)'),
    ('http://www.uwyo.edu/', 'University of Wyoming'),
    ('http://vaillibrary.com/', 'Vail Public Library'),
    ('http://library.wyo.gov/', 'Wyoming State Library')])


TRIPLESTORE_URL = "http://localhost:9999/blazegraph/sparql"
PREFIX = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>"""



def report_router(report_name):
    if report_name.startswith("institution-count"):
        return institution_counts()
    if report_name.startswith("rights-statements"):
        return rights_statements()

def institution_counts():
    output = {"total": 0, "institutions": []}
    sparql = PREFIX + """
SELECT ?institution (count(?instance) as ?instance_count)
WHERE {
     ?instance rdf:type bf:Instance .
     ?item bf:itemOf ?instance ;
           bf:heldBy ?institution .

} GROUP BY ?institution"""
    result = requests.post(TRIPLESTORE_URL,
        data={"query": sparql,
              "format": "json"})
    bindings = result.json().get('results').get('bindings')
    for row in bindings:
        institution_iri = row.get("institution").get("value")
        count = row.get("instance_count").get("value")
        output["institutions"].append(
            {"label": INSTITUTIONS.get(institution_iri),
             "url": institution_iri,
             "count": count})
        output['total'] += int(count)
    return output
        
    
def rights_statements():
    output = []
    sparql = PREFIX + """
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

