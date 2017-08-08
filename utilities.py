__author__ = "Jeremy Nelson"

import csv, datetime, uuid, rdflib, re, requests
import bibcat.rml.processor as processor
import bibcat.linkers.deduplicate as deduplicate

RANGE_4YEARS = re.compile(r"(\d{4})-(\d{4})")
RANGE_4to2YEARS = re.compile(r"(\d{4})-(\d{2})\b")
YEAR = re.compile("(\d{4})")

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

def add_dpl(**kwargs):
    graph = kwargs.get('graph')
    field = kwargs.get('field')
    row = kwargs.get('row')

def history_colo_workflow():
    hist_col_urls = dict()
    for row in csv.DictReader(open("E:/2017/Plains2PeaksPilot/input/history-colorado-urls.csv")):
        hist_col_urls[row.get("Object ID")] = {"item": row["Portal Link"],
                                               "cover": row["Image Link"]}
    
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
        base_url='https://plains2peaks.org/')

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
