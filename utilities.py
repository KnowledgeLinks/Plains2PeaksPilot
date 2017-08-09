__author__ = "Jeremy Nelson"

import csv
import datetime
import re
import sys
import uuid
import bibcat
import requests
import rdflib 
import bibcat.rml.processor as processor
import bibcat.linkers.deduplicate as deduplicate

sys.path.append("E:/2017/dpla-service-hub")
import date_generator

BASE_URL = "https://plains2peaks.org/"
RANGE_4YEARS = re.compile(r"(\d{4})-(\d{4})")
RANGE_4to2YEARS = re.compile(r"(\d{4})-(\d{2})\b")
YEAR = re.compile("(\d{4})")

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

RIGHTS_STATEMENTS = {
    'COPYRIGHT NOT EVALUATED': rdflib.URIRef("http://rightsstatements.org/vocab/CNE/1.0/"),
    'IN COPYRIGHT-EDUCATIONAL USE PERMITTED': rdflib.URIRef('http://rightsstatements.org/vocab/InC-EDU/1.0/'),
    'NO COPYRIGHT-UNITED STATES': rdflib.URIRef('http://rightsstatements.org/vocab/NoC-US/1.0/'),
    'NO KNOWN COPYRIGHT': rdflib.URIRef('http://rightsstatements.org/vocab/NKC/1.0/')
}
TRIPLESTORE_URL = 'http://localhost:9999/blazegraph/sparql'

def add_dpl(**kwargs):
    graph = kwargs.get('graph')
    field = kwargs.get('field')
    row = kwargs.get('row')

def hist_co_collections(row, bf_graph):
    raw_collection = row.get("Collection Name")
    collection_iri = rdflib.URIRef("{}{}".format(BASE_URL, 
        bibcat.slugify(raw_collection)))
    work = bf_graph.value(predicate=rdflib.RDF.type,
        object=BF.Work)
    bf_graph.add((work, BF.partOf, collection_iri))
    bf_graph.add((collection_iri, rdflib.RDF.type, BF.Collection))
    bf_graph.add((collection_iri, rdflib.RDFS.label, rdflib.Literal(raw_collection)))
    

def hist_co_subjects_process(row, bf_graph):
    work = bf_graph.value(predicate=rdflib.RDF.type,
        object=BF.Work)
    raw_subject_terms = row.get("Subject.Term")
    subject_terms = [r.strip() for r in raw_subject_terms.split(",")]
    for term in subject_terms:
        if "collection" in term.lower():
            collection_iri = bf_graph.value(subject=work,
                predicate=BF.partOf)
            temp_iri = rdflib.URIRef("{}{}".format(BASE_URL, bibcat.slugify(term)))
            if collection_iri == temp_iri:
                continue
        topic_bnode = rdflib.BNode()
        bf_graph.add((work, BF.subject, topic_bnode))
        bf_graph.add((topic_bnode, rdflib.RDF.type, BF.Topic))
        bf_graph.add((topic_bnode, rdflib.RDF.value, rdflib.Literal(term)))
    raw_local_terms = row.get("Locale.Term")
    for term in [r.strip() for r in raw_local_terms.split(",")]:
        if len(term) < 1:
            continue
        local_bnode = rdflib.BNode()
        bf_graph.add((work, BF.place, local_bnode))
        bf_graph.add((local_bnode, rdflib.RDF.type, BF.Place))
        bf_graph.add((local_bnode, rdflib.RDF.value, rdflib.Literal(term)))
    raw_used_terms = row.get("Used.Term")
    for term in [r.strip() for r in raw_used_terms.split(",")]:
        related_bnode = rdflib.BNode()
        bf_graph.add((work, BF.relatedTo, related_bnode))
        bf_graph.add((related_bnode, rdflib.RDF.type, rdflib.RDFS.Resource))
        bf_graph.add((related_bnode, rdflib.RDF.value, rdflib.Literal(term)))   



def history_colo_workflow():
    hist_col_urls = dict()
    for row in csv.DictReader(open("E:/2017/Plains2PeaksPilot/input/history-colorado-urls.csv")):
        hist_col_urls[row.get("Object ID")] = {"item": row["Portal Link"],
                                               "cover": row["Image Link"]}
    history_colo_graph = None
    start_workflow = datetime.datetime.utcnow()
    output_filename = "E:/2017/Plains2PeaksPilot/output/history-colorado.xml"
    print("Starting History Colorado Workflow at {}".format(start_workflow.isoformat()))
    for i,row in enumerate(hist_co_pilot):
        try:
            item_iri = hist_col_urls.get(row.get("Object ID")).get("item")
            csv2bf.run(row=row,
                instance_iri="{}{}".format(BASE_URL, uuid.uuid1()),
                item_iri=item_iri)
            hist_co_collections(row, csv2bf.output)
            hist_co_subjects_process(row, csv2bf.output)
            p2p_date_generator = date_generator.DateGenerator(graph=csv2bf.output)
            p2p_date_generator.run(row.get("Dates.Date Range"))
            rights_stmt = row.get("DPLA Rights").upper()
            if rights_stmt in RIGHTS_STATEMENTS:
                csv2bf.output.add((rdflib.URIRef(item_iri), 
                                   BF.AccessPolicy, 
                                   RIGHTS_STATEMENTS.get(rights_stmt)))
            p2p_deduplicator.run(csv2bf.output, [BF.Agent, 
                                                 BF.Person, 
                                                 BF.Organization, 
                                                 BF.Place,
                                                 BF.Topic])
            # Ingest into triplestore
            #result = requests.post(TRIPLESTORE_URL,
            #    data=csv2bf.output.serialize(),
            #    headers={"Content-Type": "application/rdf+xml"})
        except:
            print("E{:,} ".format(i), end="")
            print(sys.exc_info()[1], end=" ")
            continue

        if not i%10 and i > 0:
            print(".", end="")
        if not i%100:
            print("{:,}".format(i), end="")
        if not i%250 and i > 0:
            with open(output_filename, "wb+") as fo:
                fo.write(history_colo_graph.serialize())
        if history_colo_graph is None:
            history_colo_graph = csv2bf.output
        else:
            history_colo_graph += csv2bf.output
    end_workflow = datetime.datetime.utcnow()
    with open(output_filename, "wb+") as fo:
        fo.write(history_colo_graph.serialize())
    print("Finished History Colorado at {}, total time {} minutes for {:,} objects".format(
        end_workflow.isoformat(),
        (end_workflow-start_workflow).seconds / 60.0,
        i))
    
    
    
    
def marmot_workflow(marmot_url):
	start = datetime.datetime.utcnow()
	print("Started Marmot Harvest at {}".format(start))
	initial_graph, total_pages = temp_marmot(marmot_url)
	for page in range(2, total_pages+1):
		shard_url = "{}&page={}".format(marmot_url,
						page)
		if not page%5:
			print(".", end="")
		if not page%10:
			with open("E:/2017/Plains2PeaksPilot/output/marmot-{}-{}.ttl".format(page-10, page), "wb+") as fo:
				fo.write(initial_graph.serialize(format='turtle'))
			initial_graph = None
			print(page, end="")
		if initial_graph is None:
			initial_graph = temp_marmot(shard_url)[0]
		else:
			initial_graph += temp_marmot(shard_url)[0]
	with open("E:/2017/Plains2PeaksPilot/output/marmot-{}-final.ttl".format(page),
		  "wb+") as fo:
		fo.write(initial_graph.serialize(format='turtle'))
	end = datetime.datetime.utcnow()
	print("Finished at {}, total time {} mins".format(
		end.isoformat(),
		(end-start).seconds / 60.0))



 
def setup_hist_co():
    global csv2bf, hist_co_pilot, p2p_deduplicator
    hist_co_pilot = csv.DictReader(open("E:/2017/Plains2PeaksPilot/input/history-colorado-2017-07-11.csv"))
    csv2bf = processor.CSVRowProcessor(rml_rules=['bibcat-base.ttl',
        'E:/2017/dpla-service-hub/profiles/history-colo-csv.ttl'])
    p2p_deduplicator = deduplicate.Deduplicator(
        triplestore_url='http://localhost:9999/blazegraph/sparql',
        base_url=BASE_URL)

def temp_marmot(url):
    result = requests.get(url)
    marmot_json = result.json()
    docs = marmot_json['result']['docs']
    bf_graph = rdflib.Graph()
    bf_graph.namespace_manager.bind("bf", BF)
    for doc in docs:
        instance_uri = rdflib.URIRef("https://plains2peaks.org/{}".format(
            uuid.uuid1()))
        bf_graph.add((instance_uri, rdflib.RDF.type, BF.Instance))
        work_uri = rdflib.URIRef("{}#work".format(instance_uri))
        bf_graph.add((work_uri, rdflib.RDF.type, BF.Item))
        bf_graph.add((instance_uri, BF.instanceOf, work_uri))
        item_uri = rdflib.URIRef(doc.get('isShownAt'))
        bf_graph.add((item_uri, BF.itemOf, instance_uri))
        bf_graph.add((item_uri, rdflib.RDF.type, BF.Item))
        cover_art = rdflib.BNode()
        bf_graph.add((cover_art, rdflib.RDF.type, BF.CoverArt))
        bf_graph.add((instance_uri, BF.coverArt, cover_art))
        bf_graph.add((cover_art, rdflib.RDF.value,
            rdflib.URIRef(doc.get('preview'))))
        institution = rdflib.BNode()
        bf_graph.add((institution, rdflib.RDF.type, BF.Organization))
        bf_graph.add((institution, rdflib.RDFS.label,
            rdflib.Literal(doc.get('dataProvider'))))
        publication = rdflib.BNode()
        bf_graph.add((publication, rdflib.RDF.type, BF.Publication))
        bf_graph.add((publication, BF.agent, institution))
        bf_graph.add((instance_uri, BF.provisionActivity, publication))
        distribution = rdflib.BNode()
        bf_graph.add((distribution, rdflib.RDF.type, BF.Distribution))
        bf_graph.add((distribution, BF.agent, rdflib.URIRef("https://marmot.org/")))
        bf_graph.add((instance_uri, BF.provisionActivity, distribution))
        title = rdflib.BNode()
        title_label = rdflib.Literal(doc.get('title'))
        bf_graph.add((title, rdflib.RDF.type, BF.Title))
        bf_graph.add((title, rdflib.RDFS.label,
            title_label))
        bf_graph.add((title, BF.mainTitle, title_label)) 
        bf_graph.add((instance_uri, BF.title, title))
        for row in doc.get('place', []):
            place = rdflib.BNode()
            bf_graph.add((place, rdflib.RDF.type, BF.Place))
            bf_graph.add((place, rdflib.RDF.value,
                                   rdflib.Literal(row)))
            bf_graph.add((work_uri, BF.subject, place))
 
        right = rdflib.BNode()
        bf_graph.add((instance_uri, BF.usageAndAccessPolicy, right))
        bf_graph.add((right, rdflib.RDF.type, BF.UsageAndAccessPolicy))
        bf_graph.add((right, rdflib.RDF.value,
                               rdflib.Literal(doc.get('rights'))))
        for row in doc.get('subject', []):
            subject = rdflib.BNode()
            label = rdflib.Literal(row)
            bf_graph.add((subject, rdflib.RDF.type, BF.Topic))
            bf_graph.add((work_uri, BF.subject, subject))
            bf_graph.add((subject, rdflib.RDF.value, label))
        for row in doc.get('creator', []):
            creator = rdflib.BNode()
            label = rdflib.Literal(row)
            bf_graph.add((creator, rdflib.RDF.type, BF.Agent))
            bf_graph.add((creator, rdflib.RDF.value, label))
            bf_graph.add((work_uri, rdflib.URIRef('http://id.loc.gov/vocabulary/relators/cre.html'), creator))
 
        description = doc.get('description')
        if len(description) > 0:
            summary = rdflib.BNode()
            bf_graph.add((summary, rdflib.RDF.type, BF.Summary))
            bf_graph.add((summary, rdflib.RDF.value,
                       rdflib.Literal(description)))
            bf_graph.add((work_uri, BF.summary, summary))
        class_ = doc.get('format').replace(" ", "")
        bf_addl_class = getattr(BF, class_)
        bf_graph.add((work_uri, rdflib.RDF.type, bf_addl_class))
        for row in doc.get('publisher', []):
            manufacture = rdflib.BNode()
            bf_graph.add((manufacture, rdflib.RDF.type, BF.Manufacture))
            bf_graph.add((instance_uri, BF.provisionActivity, manufacture))
            agent = rdflib.BNode()
            bf_graph.add((manufacture, BF.agent, agent))
            bf_graph.add((agent, rdflib.RDF.type, BF.Agent))
            bf_graph.add((agent, rdflib.RDF.value,
                       rdflib.Literal(row)))
        carrier_type = rdflib.BNode()
        bf_graph.add((carrier_type, rdflib.RDF.type, BF.Carrier))
        bf_graph.add((instance_uri, BF.carrier, carrier_type))
        bf_graph.add((carrier_type, rdflib.RDF.value,
               rdflib.Literal(doc.get('format'))))
        ident = rdflib.BNode()
        bf_graph.add((ident, rdflib.RDF.type, BF.Local))
        bf_graph.add((ident, rdflib.RDF.value,
               rdflib.Literal(doc.get('identifier'))))
        bf_graph.add((instance_uri, BF.identifiedBy, ident))
        for row in doc.get('relation', []):
            collection = rdflib.BNode()
            bf_graph.add((collection, rdflib.RDF.type,
                       BF.Collection))
            bf_graph.add((collection, rdflib.RDFS.label,
                       rdflib.Literal(row)))
            bf_graph.add((work_uri, BF.partOf, collection))
    return bf_graph

class DateGenerator(object):
    """Class dates a raw string and attempts to generate RDF associations"""

    def __init__(self, **kwargs):
        self.graph = kwargs.get("graph")
        self.work = self.graph.value(predicate=rdflib.RDF.type,
                                     object=BF.Work)
        if self.work is None:
            raise ValueError("Work missing from graph")


    def add_range(self, start, end):
        for date_row in range(int(start), int(end)+1):
            self.add_year(date_row)
    
    def add_4_years(self, result):
        self.graph.add((
            self.work,
            BF.temporalCoverage,
            rdflib.Literal(result.string)))
        start, end = result.groups()
        self.add_range(start, end)

    def add_4_to_2_years(self, result):
        start_year, stub_year = result.groups()
        end_year = "{}{}".format(start_year[0:2], stub_year)
        self.graph.add((self.work, 
                        BF.temporalCoverage, 
                        rdflib.Literal("{} to {}".format(start_year, end_year))))
        self.add_range(start_year, end_year)

    def add_year(self, year):
        bnode = rdflib.BNode()
        self.graph.add((self.work, BF.subject, bnode))
        self.graph.add((bnode, rdflib.RDF.type, BF.Temporal))
        self.graph.add((bnode, rdflib.RDF.value, rdflib.Literal(year)))

    def run(self, raw_date):
        if len(raw_date) == 4 and YEAR.search(raw_date):
            self.add_year(raw_date)
        if "," in raw_date:
            for comma_row in raw_date.split(","):
                self.run(comma_row.strip())
        else:
            result = RANGE_4YEARS.search(raw_date)
            if result is not None:
                self.add_4_years(result)
            result = RANGE_4to2YEARS.search(raw_date)
            if result is not None:
                self.add_4_to_2_years(result)
