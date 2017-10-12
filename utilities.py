__author__ = "Jeremy Nelson"

import csv
import datetime
import logging
import os
import re
import sys
import time
import uuid
import bibcat
import requests
import rdflib 
import bibcat.rml.processor as processor
import bibcat.linkers.deduplicate as deduplicate
import bibcat.linkers.geonames as geonames
import bibcat.linkers.loc as loc
import bibcat.ingesters.oai_pmh as ingesters
from bibcat.ingesters.rels_ext import RELSEXTIngester

import lxml.etree

if sys.platform.startswith("win"):
    sys.path.append("E:/2017/dpla-service-hub")
    error_log  = "E:/2017/Plains2PeaksPilot/errors/error-{}.log".format(time.monotonic())
else:
    sys.path.append("/Users/jeremynelson/2017/dpla-service-hub")
    error_log = "/Users/jeremynelson/2017/Plains2PeaksPilot/errors/error-{}.log".format(time.monotonic())
import date_generator

logging.basicConfig(filename="ingestion-{}.log".format(time.monotonic()), 
                    level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filemode="w")

BASE_URL = "https://plains2peaks.org/"
RANGE_4YEARS = re.compile(r"(\d{4})-(\d{4})")
RANGE_4to2YEARS = re.compile(r"(\d{4})-(\d{2})\b")
YEAR = re.compile("(\d{4})")

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
RELATORS = rdflib.Namespace("http://id.loc.gov/vocabulary/relators/")
SCHEMA = rdflib.Namespace("http://schema.org/")
SKOS = rdflib.Namespace("http://www.w3.org/2004/02/skos/core#")

RIGHTS_STATEMENTS = {
    'COPYRIGHT NOT EVALUATED': rdflib.URIRef("http://rightsstatements.org/vocab/CNE/1.0/"),
    'IN COPYRIGHT': rdflib.URIRef("http://rightsstatements.org/vocab/InC/1.0/"),
    'IN COPYRIGHT-EDUCATIONAL USE PERMITTED': rdflib.URIRef('http://rightsstatements.org/vocab/InC-EDU/1.0/'),
    'NO COPYRIGHT-UNITED STATES': rdflib.URIRef('http://rightsstatements.org/vocab/NoC-US/1.0/'),
    'NO KNOWN COPYRIGHT': rdflib.URIRef('http://rightsstatements.org/vocab/NKC/1.0/')
}
TRIPLESTORE_URL = 'http://localhost:9999/blazegraph/sparql'

P2P_DEDUPLICATOR = deduplicate.Deduplicator(
    triplestore_url=TRIPLESTORE_URL,
    base_url=BASE_URL,
    classes=[BF.Agent, BF.Organization, BF.Person, BF.Topic])

LOC_DEDUP = loc.LibraryOfCongressLinker()
    
def add_dpl(**kwargs):
    graph = kwargs.get('graph')
    field = kwargs.get('field')
    row = kwargs.get('row')

def __amer_heritage_add_collection__(bf_graph, collection_iri):
    for work_iri in bf_graph.subjects(predicate=rdflib.RDF.type,
        object=BF.Work):
        bf_graph.add((work_iri, BF.partOf, collection_iri))
    for item_iri in bf_graph.subjects(predicate=rdflib.RDF.type,
            object=BF.Item):
        held_by = bf_graph.value(subject=item_iri, predicate=BF.heldBy)
        if held_by is None:
            bf_graph.add((item_iri, BF.heldBy, amer_iri))

def amer_heritage_workflow(out_file):
    def __setup__():
        global collections, luna_harvester, amer_graph, amer_iri
        collections = [("uwydbuwy~22~22", rdflib.URIRef("http://digitalcollections.uwyo.edu/luna/servlet/uwydbuwy~22~22")), 
                       ("uwydbuwy~96~96", rdflib.URIRef("http://digitalcollections.uwyo.edu/luna/servlet/uwydbuwy~96~96")),
                       ("uwydbuwy~148~148", rdflib.URIRef("http://digitalcollections.uwyo.edu/luna/servlet/uwydbuwy~148~148"))]
        luna_harvester = ingesters.LunaIngester(
            repository='http://digitalcollections.uwyo.edu/luna/servlet/oai',
            base_url=BASE_URL)
        amer_iri = rdflib.URIRef("http://www.uwyo.edu/ahc/")
    __setup__()
    start = datetime.datetime.utcnow()
    print("Starting American Heritage Harvester")
    amer_graph = None
    for collection in collections:
        luna_harvester.harvest(setSpec=collection[0], 
            instance_iri=lambda: "{}/{}".format(BASE_URL, uuid.uuid1()))
        __amer_heritage_add_collection__(luna_harvester.repo_graph, collection[1])
        if amer_graph is None:
            amer_graph = luna_harvester.repo_graph
        else:
            amer_graph += luna_harvester.repo_graph
        

    with open(out_file, "wb+") as fo:
        fo.write(amer_graph.serialize(format='turtle'))
    end = datetime.datetime.utcnow()
    print("Finished at {}, total time {} number of triples {}".format(
        end,
        (end-start).seconds / 60.0,
        len(amer_graph)))
    return amer_graph

def __cc_collection__(pid, bf_graph, rights_stmt=RIGHTS_STATEMENTS["IN COPYRIGHT"]):
    def set_label(pid):
        mods_url = "{}{}/datastream/MODS".format(cc_repo_base, pid)
        result = requests.get(mods_url)
        if result.status_code > 399:
            return
        mods_xml = lxml.etree.XML(result.text)
        title = mods_xml.xpath("mods:titleInfo/mods:title", 
            namespaces=cc_processor.xml_ns)
        if title is None:
            return
        bf_graph.add((collection_iri, 
            rdflib.RDFS.label, 
            rdflib.Literal(title[0].text, lang="en"))) 
    child_results = requests.post(fedora_ri_search,
        data={"type": "tuples",
              "lang": "sparql",
              "format": "json",
              "query": """SELECT DISTINCT ?s
WHERE {{
      ?s <fedora-rels-ext:isMemberOfCollection> <info:fedora/{}> .
}}""".format(pid)},
        auth=fedora_auth)
    if child_results.status_code > 399:
        raise ValueError("Could not add CC collection")
    collection_iri = rdflib.URIRef("{}{}".format(cc_repo_base, pid))
    bf_graph.add((collection_iri, rdflib.RDF.type, BF.Collection))
    set_label(pid)
    count = 0
    start = datetime.datetime.utcnow()
    print("Start processing collection {} at {}".format(
        pid,
        start))
    for i,child_row in enumerate(child_results.json().get("results")):
        child_pid = child_row.get("s").split("/")[-1]
        if __cc_is_collection__(child_pid):
            __cc_collection__(child_pid, bf_graph, rights_stmt)
            continue
        item_iri = __cc_pid__(child_pid, bf_graph)
        if item_iri is None:
            continue
        bf_graph.add((item_iri, BF.usageAndAccessPolicy, rights_stmt))
        instance_iri = bf_graph.value(subject=item_iri,
            predicate=BF.itemOf)
        work_iri = bf_graph.value(subject=instance_iri,
            predicate=BF.instanceOf)
        bf_graph.add((work_iri, BF.partOf, collection_iri))
        if not i%5 and i>0:
            print('.', end="")
        if not i%10:
            print(i, end="")
        count += 1
    end = datetime.datetime.utcnow()
    print("Finished processing at {}, total {} mins, {} objects for PID {}".format(
        end,
        (end-start).seconds / 60.0,
        count,
        pid)) 

def __cc_is_collection__(pid):
    sparql = """SELECT DISTINCT ?o
WHERE {{        
  <info:fedora/{0}> <fedora-model:hasModel> <info:fedora/islandora:collectionCModel> .
  <info:fedora/{0}> <fedora-model:hasModel> ?o
}}""".format(pid)
    collection_result = requests.post(fedora_ri_search,
        data={"type": "tuples",
              "lang": "sparql",
              "format": "json",
              "query": sparql},
        auth=fedora_auth)
    if len(collection_result.json().get('results')) > 0:
        return True
    return False

def __cc_is_member__(pid):
    rels_ext_url = "{}{}/datastream/RELS-EXT".format(
            cc_repo_base,
            pid)
    rels_ext_result = requests.get(rels_ext_url)
    if rels_ext_result.status_code < 399:
        rels_ext_xml = lxml.etree.XML(rels_ext_result.text)
        is_constituent =  rels_ext_xml.xpath(
            "rdf:Description/fedora:isConstituentOf",
            namespaces=rels_processor.xml_ns)
        if len(is_constituent) > 0:
            return True
    return False
        
def __cc_pid__(pid, bf_graph):
    if __cc_is_member__(pid):
        return
    mods_url = "{}{}/datastream/MODS".format(cc_repo_base, pid)
    
    item_iri = rdflib.URIRef("{}{}".format(cc_repo_base, pid))
    instance_iri = "{}{}".format(BASE_URL, uuid.uuid1())
    mods_result = requests.get(mods_url)
    mods_xml = mods_result.text
    #if isinstance(mods_xml, str):
    #    mods_xml = mods_xml.encode()
    cc_processor.run(mods_xml, 
        instance_iri=instance_iri,
        item_iri=item_iri)
    if bf_graph is None:
        bf_graph = cc_processor.output
    else:
        bf_graph += cc_processor.output
    held_by = bf_graph.value(subject=item_iri, predicate=BF.heldBy)
    if held_by is None:
        bf_graph.add((item_iri, 
                      BF.heldBy, 
                      rdflib.URIRef("https://www.coloradocollege.edu/")))
    work_uri = bf_graph.value(subject=rdflib.URIRef(instance_iri),
            predicate=BF.instanceOf)
    if work_uri is None:
        work_uri = rdflib.URIRef("{}#Work".format(instance_iri))
        bf_graph.add((work_uri, rdflib.RDF.type, BF.Work))
        bf_graph.add((rdflib.URIRef(instance_iri), BF.instanceOf, work_uri))
    rels_url = "{}{}/datastream/RELS-EXT".format(cc_repo_base, pid)
    rels_result = requests.get(rels_url)
    rels_processor.run(rels_result.text,
        instance_iri=instance_iri,
        work_iri=str(work_uri))
    bf_graph += rels_processor.output
    P2P_DEDUPLICATOR.run(bf_graph)
    return item_iri

def colorado_college_workflow(**kwargs):
    global cc_processor, rels_processor, fedora_ri_search, fedora_auth, cc_repo_base
    cc_processor = processor.XMLProcessor(
        rml_rules=["bibcat-base.ttl",
            "mods-to-bf.ttl",
            kwargs.get("cc_rules")],
        triplestore_url=TRIPLESTORE_URL,
        base_url=BASE_URL,
        namespaces={"mods": "http://www.loc.gov/mods/v3",
                    "xlink": "https://www.w3.org/1999/xlink"})
    rels_processor = RELSEXTIngester(base_url=BASE_URL)    
    fedora_ri_search = kwargs.get("ri_search")
    fedora_auth = kwargs.get("auth")
    cc_repo_base = kwargs.get("repo_base")

def __hist_co_collections__(row, bf_graph):
    raw_collection = row.get("Collection Name")
    collection_iri = rdflib.URIRef("{}{}".format(BASE_URL, 
        bibcat.slugify(raw_collection)))
    work = bf_graph.value(predicate=rdflib.RDF.type,
        object=BF.Work)
    bf_graph.add((work, BF.partOf, collection_iri))
    bf_graph.add((collection_iri, rdflib.RDF.type, BF.Collection))
    bf_graph.add((collection_iri, rdflib.RDFS.label, rdflib.Literal(raw_collection)))
    
def __hist_co_cover__(instance_url, cover_art_url, bf_graph):
    instance_iri = rdflib.URIRef(instance_url)
    cover_art_iri = rdflib.URIRef(cover_art_url)
    cover_art_bnode = rdflib.BNode()
    bf_graph.add((instance_iri, BF.coverArt, cover_art_bnode))
    bf_graph.add((cover_art_bnode, rdflib.RDF.type, BF.CoverArt))
    bf_graph.add((cover_art_bnode, rdflib.RDF.value, cover_art_iri))

def __hist_co_subjects_process__(row, bf_graph):
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

def __process_hist_colo_row__(row):
    item_iri = hist_col_urls.get(row.get("Object ID")).get("item")
    instance_url = "{}{}".format(BASE_URL, uuid.uuid1())
    cover_art_url = hist_col_urls.get(row.get("Object ID")).get('cover')
    csv2bf.run(row=row,
        instance_iri=instance_url,
        item_iri=item_iri)
    __hist_co_cover__(instance_url, cover_art_url, csv2bf.output)
    __hist_co_collections__(row, csv2bf.output)
    __hist_co_subjects_process__(row, csv2bf.output)
      
    p2p_date_generator = date_generator.DateGenerator(graph=csv2bf.output)
    p2p_date_generator.run(row.get("Dates.Date Range"))
    rights_stmt = row.get("DPLA Rights").upper()
    if rights_stmt in RIGHTS_STATEMENTS:
        csv2bf.output.add((rdflib.URIRef(item_iri), 
                           BF.usageAndAccessPolicy, 
                           RIGHTS_STATEMENTS.get(rights_stmt)))
    P2P_DEDUPLICATOR.run(csv2bf.output, [BF.Agent, 
                                         BF.Person, 
                                         BF.Organization, 
                                         BF.Topic])
    return csv2bf.output



def history_colo_workflow():
    history_colo_graph = None
    start_workflow = datetime.datetime.utcnow()
    output_filename = "E:/2017/Plains2PeaksPilot/output/history-colorado.xml"
    print("Starting History Colorado Workflow at {}".format(start_workflow.isoformat()))
    for i,row in enumerate(hist_co_pilot):
        try:
            row_graph = __process_hist_colo_row__(row)
            #result = requests.post(TRIPLESTORE_URL,
            #    data=csv2bf.output.serialize(),
            #    headers={"Content-Type": "application/rdf+xml"})
        except:
            print("E{:,} ".format(i), end="")
            print(sys.exc_info()[1], end=" ")
            logging.error("{} History Colorado - record {}, {}".format(
                time.monotonic(),
                i,
                sys.exc_info()[1]))
            continue

        if not i%10 and i > 0:
            print(".", end="")
        if not i%100:
            print("{:,}".format(i), end="")
        if not i%250 and i > 0:
            with open(output_filename, "wb+") as fo:
                fo.write(history_colo_graph.serialize())
        if history_colo_graph is None:
            history_colo_graph = row_graph 
        else:
            history_colo_graph += row_graph
    end_workflow = datetime.datetime.utcnow()
    with open(output_filename, "wb+") as fo:
        fo.write(history_colo_graph.serialize())
    print("Finished History Colorado at {}, total time {} minutes for {:,} objects".format(
        end_workflow.isoformat(),
        (end_workflow-start_workflow).seconds / 60.0,
        i))
    
def __marmot_setup__(org_file):   
    global marmot_orgs, marmot_orgs_dict, org_filepath 
    org_filepath = org_file
    marmot_orgs_dict = dict()
    marmot_orgs = rdflib.Graph()
    marmot_orgs.parse(org_file, format='turtle')
    for library_iri in marmot_orgs.subjects(predicate=rdflib.RDF.type,
        object=SCHEMA.Library):
        label = marmot_orgs.value(subject=library_iri, predicate=rdflib.RDFS.label)
        marmot_orgs_dict[str(label)] = library_iri
    for library_iri in marmot_orgs.subjects(predicate=rdflib.RDF.type,
        object=SCHEMA.Library):
        label = marmot_orgs.value(subject=library_iri, predicate=rdflib.RDFS.label)
        marmot_orgs_dict[str(label)] = library_iri


def marmot_workflow(marmot_url, org_file, total_pages=85):
    __marmot_setup__(org_file)
    start = datetime.datetime.utcnow()
    print("Started Marmot Harvest at {}, total pages = {} ".format(start, total_pages))
    for page in range(1, total_pages+1):
        shard_url = "{}&page={}".format(marmot_url,
                                        page)
        print(".", end="")
        initial_graph = temp_marmot(shard_url)[0]
        with open("E:/2017/Plains2PeaksPilot/output/marmot-{}.ttl".format(page), "wb+") as fo:
            fo.write(initial_graph.serialize(format='turtle'))
        if not page%10:
            print(page, end="")
    end = datetime.datetime.utcnow()
    print("Finished at {}, total time {} mins".format(
            end.isoformat(),
            (end-start).seconds / 60.0))

def __add_univ_wy_collection__(bf_graph, collection_pid):
    collection_iri = wy_collection_iri.get(collection_pid)
    for work_iri in bf_graph.subjects(predicate=rdflib.RDF.type,
                                      object=BF.Work):
        bf_graph.add((work_iri, BF.partOf, collection_iri))
    

def __univ_wy_covers__(bf_graph):
    for item_iri in bf_graph.subjects(predicate=rdflib.RDF.type,
        object=BF.Item):
        item_url = str(item_iri)
        cover_tn_url = "{}datastream/TN".format(item_url)
        cover_exists = requests.get(cover_tn_url)
        if cover_exists.status_code < 400:
            instance_iri = bf_graph.value(subject=item_iri, predicate=BF.itemOf)
            cover_bnode = rdflib.BNode()
            bf_graph.add((instance_iri, BF.coverArt, cover_bnode))
            bf_graph.add((cover_bnode, rdflib.RDF.type, BF.CoverArt))
            bf_graph.add((cover_bnode, rdflib.RDF.value, rdflib.URIRef(cover_tn_url)))
        held_by = bf_graph.value(subject=item_iri, predicate=BF.heldBy)
        if held_by is None:
            bf_graph.add((item_iri, BF.heldBy, wy_iri))
        
   
def __univ_wy_periodicals__(pid):
    pid_url ="https://uwdigital.uwyo.edu/islandora/object/{pid}/".format(
        pid=pid)
    mods_url = "{}datastream/MODS".format(pid_url)
    mods_result = requests.get(mods_url)
    mods_ingester.run(mods_result.text,
        instance_iri="{}/{}".format(BASE_URL, uuid.uuid1()),
        item_iri=pid_url)
    bf_dedup.run(mods_ingester.output, 
        [BF.Person, 
         BF.Agent,
         BF.Topic,
         BF.Organization])
    return mods_ingester.output

def univ_wy_workflow(out_file):
    setup_univ_wy()
    start = datetime.datetime.utcnow()
    print("Starting University of Wyoming Workflow using Islandora OAI-PMH at {}".format(
        start.isoformat()))
    univ_wy_graph = rdflib.Graph()
    if os.path.exists(out_file):
        univ_wy_graph.parse(out_file, format='turtle')
    else:
        univ_wy_graph.namespace_manager.bind("bf", BF)
        univ_wy_graph.namespace_manager.bind("relators", RELATORS)
        univ_wy_graph.namespace_manager.bind("schema", SCHEMA)
        univ_wy_graph.namespace_manager.bind("skos", SKOS)
    for collection_pid in wy_collections:
        start_size = len(univ_wy_graph) 
        i_harvester.harvest(setSpec=collection_pid, dedup=bf_dedup)
        __univ_wy_covers__(i_harvester.repo_graph)
        __add_univ_wy_collection__(i_harvester.repo_graph, collection_pid)
        univ_wy_graph += i_harvester.repo_graph
        msg = "Finished collection {} number of triples {:,}".format(collection_pid, 
            len(univ_wy_graph) - start_size)
        logging.info(msg)
        print("====\n{}".format(msg))
        #with open(out_file, 'wb+') as fo:
        #    fo.write(univ_wy_graph.serialize(format='turtle'))
    for periodical_pid in wy_periodicals:
        periodical_graph = __univ_wy_periodicals__(periodical_pid)
        msg = "Finished periodical {}, number of triples {:,}".format(
            periodical_pid,
            len(periodical_graph))
        logging.info(msg)
        print("=====\n{}".format(periodical_pid))
        univ_wy_graph += periodical_graph
    with open(out_file, "wb+") as fo:
        fo.write(univ_wy_graph.serialize(format='turtle'))
    end = datetime.datetime.utcnow()
    print("""Finished University of Wyoming pilot at {}
Total number of triples: {} 
             Total time: {} minutes""".format(end.isoformat(),
        len(univ_wy_graph),
        (end-start).seconds / 60.0)) 

def __wy_state_collections__(raw_name, bf_graph, existing_collections):
    if raw_name in existing_collections:
        return existing_collections.get('raw_name')
    collection_iri = rdflib.URIRef("{}wy-state/{}".format(
        BASE_URL, bibcat.slugify(raw_name)))
    first_type = bf_graph.value(subject=collection_iri,
        predicate=rdflib.RDF.type)
    if first_type is None:
        bf_graph.add((collection_iri, rdflib.RDF.type, BF.Collection))
        bf_graph.add((collection_iri, rdflib.RDFS.label, rdflib.Literal(raw_name)))
    existing_collections[raw_name] = collection_iri
    return collection_iri
    
    


def wy_state_workflow(**kwargs):
    source_dir = kwargs.get('source')
    out_file = kwargs.get('out_file')
    wy_state_rule = kwargs.get('wy_rule')
    def __setup__():
        global ptfs_processor, p2p_deduplicator
        ptfs_processor = processor.XMLProcessor(
            triplestore_url=TRIPLESTORE_URL,
            base_url=BASE_URL,
            rml_rules = ['bibcat-base.ttl',
                         'bibcat-ptfs-to-bf.ttl',
                         wy_state_rule])
        p2p_deduplicator = deduplicate.Deduplicator(
            triplestore_url=TRIPLESTORE_URL,
            base_url=BASE_URL)
    __setup__()     
    start = datetime.datetime.utcnow()
    print("Starting Wyoming State Library at {}".format(start.isoformat()))
    wy_state_graph = rdflib.Graph()
    wy_state_graph.namespace_manager.bind("bf", BF)
    collections = {}
    counter = 0
    for root, dirs, files in os.walk(source_dir):
        root_name = root.split(source_dir)[-1]
        if len(root_name) < 1:
            parent_collection = None
        else:
            if root_name.startswith("\\"):
                root_name = root_name[1:]
            if not root_name in collections:
                parent_collection = __wy_state_collections__(root_name,
                    wy_state_graph,
                    collections)
            else:
                parent_collection = collections.get(root_name)
        #print("\nStarting {}".format(parent_collection or root))
        for directory in dirs:
            collection_iri = collections.get(directory)
            if collection_iri is None:
                # Check or Create a collection IRI if doesn't exist
                collection_iri = __wy_state_collections__(directory, wy_state_graph, collections)
            if parent_collection is not None:
                wy_state_graph.add((collection_iri, BF.partOf, parent_collection))
        for i,file_name in enumerate(files):
            xml_path = os.path.join(root, file_name)
            counter += 1
            if os.path.exists(xml_path):
                instance_uri = "{}{}".format(BASE_URL, uuid.uuid1())
                with open(xml_path) as fo:
                    raw_xml = fo.read()
                xml_record = lxml.etree.XML(raw_xml.encode())
                try:
                    ptfs_processor.run(xml_record, 
                        instance_iri=instance_uri)
                except AssertionError:
                    print("E{}".format(counter), end="")
                    logging.error("{} Wyoming State Library - file {}, error={}".format(
                        time.monotonic(),
                        xml_path,
                        "Assertion Error"))
                    continue
                p2p_deduplicator.run(ptfs_processor.output,
                                     [BF.Agent, 
                                      BF.Person, 
                                      BF.Organization, 
                                      BF.Topic])
                instance_iri = rdflib.URIRef(instance_uri)
                work_iri = ptfs_processor.output.value(subject=instance_iri,
                    predicate=BF.instanceOf)
                ptfs_processor.output.add((work_iri, BF.partOf, parent_collection))
                wy_state_graph += ptfs_processor.output
                if not counter%10 and counter >0:
                    print(".", end="")
                if not counter%100:
                    print(counter, end="")
    with open(out_file, 'wb+') as fo:
        fo.write(wy_state_graph.serialize(format='turtle'))
    end = datetime.datetime.utcnow()
    print("Finished at {}, took {} minutes for {} total PTFS XML records" .format(
        end.isoformat(),
        (end-start).seconds / 60.0,
        counter))

    
    

 
def setup_hist_co():
    global csv2bf, hist_co_pilot, p2p_deduplicator, hist_col_urls
    hist_co_pilot = csv.DictReader(open("E:/2017/Plains2PeaksPilot/input/history-colorado-2017-07-11.csv"))
    csv2bf = processor.CSVRowProcessor(rml_rules=['bibcat-base.ttl',
        'E:/2017/dpla-service-hub/profiles/history-colo-csv.ttl'])
    p2p_deduplicator = deduplicate.Deduplicator(
        triplestore_url='http://localhost:9999/blazegraph/sparql',
        base_url=BASE_URL)
    hist_col_urls = dict()
    for row in csv.DictReader(open("E:/2017/Plains2PeaksPilot/input/history-colorado-urls.csv")):
        hist_col_urls[row.get("Object ID")] = {"item": row["Portal Link"],
                                               "cover": row["Image Link"]}
def setup_univ_wy():
    global wy_collections, wy_periodicals, i_harvester, bf_dedup, mods_ingester
    global wy_collection_iri, wy_iri, univ_wy_graph
    wy_iri = rdflib.URIRef("http://www.uwyo.edu/")
    # Finished 'wyu_12113',
    wy_collections = ['wyu_12113', 'wyu_5359', 'wyu_5394', 'wyu_2807', 'wyu_161514']
    wy_collection_iri = {'wyu_12113': rdflib.URIRef('https://uwdigital.uwyo.edu/islandora/object/wyu:12113'),
        'wyu_5359': rdflib.URIRef('https://uwdigital.uwyo.edu/islandora/object/wyu:5359'), 
        'wyu_5394': rdflib.URIRef('https://uwdigital.uwyo.edu/islandora/object/wyu:5394'), 
        'wyu_2807': rdflib.URIRef('https://uwdigital.uwyo.edu/islandora/object/wyu:2807'), 
        'wyu_161514': rdflib.URIRef('https://uwdigital.uwyo.edu/islandora/object/wyu:161514')}

#    wy_collections = ['wyu_5359']
    wy_periodicals = [
        'wyu:12541',
        'wyu:168429',
        'wyu:169935'
    ]
    bf_dedup = deduplicate.Deduplicator(triplestore_url=TRIPLESTORE_URL,
                                        classes=[BF.Agent, BF.Person, BF.Organization, BF.Topic],
                                        base_url='https://plains2peaks.org/')
    mods_ingester = processor.XMLProcessor(
        rml_rules = ['bibcat-base.ttl', 'mods-to-bf.ttl'],
        triplestore_url=TRIPLESTORE_URL,
        base_url=BASE_URL,
        namespaces={"mods": "http://www.loc.gov/mods/v3",
                    "xlink": "http://www.w3.org/1999/xlink"}) 
    i_harvester = ingesters.IslandoraIngester(
            triplestore_url=TRIPLESTORE_URL,
            base_url=BASE_URL,
            dedup=bf_dedup,
            repository='https://uwdigital.uwyo.edu/')

def __marmot_orgs__(label):
    library_iri = marmot_orgs_dict.get(label)
    if library_iri is None:
        print("{} not found in marmot_orgs".format(label))
        new_iri = rdflib.URIRef(input(">> new library iri"))
        marmot_orgs_dict[str(label)] = new_iri
        marmot_orgs.add((new_iri, rdflib.RDF.type, BF.Organization))
        marmot_orgs.add((new_iri, rdflib.RDF.type, SCHEMA.Library))
        marmot_orgs.add((new_iri, rdflib.RDFS.label, rdflib.Literal(label, lang="en")))
        with open(org_filepath, "wb+") as fo:
            fo.write(marmot_orgs.serialize(format='turtle'))
        library_iri = new_iri
    return library_iri 

def __generation_process__(resource_iri, graph):
    gen_process = rdflib.BNode()
    graph.add((resource_iri, BF.generationProcess, gen_process))
    graph.add((gen_process, rdflib.RDF.type, BF.GenerationProcess))
    graph.add((gen_process, BF.generationDate, rdflib.Literal(datetime.datetime.utcnow().isoformat())))
    graph.add((gen_process, 
               rdflib.RDF.value, 
               rdflib.Literal("Generated by BIBCAT version i{} from KnowledgeLinks.io".format(bibcat.__version__),
                  lang="en")))
    

def temp_marmot(url):
    result = requests.get(url)
    if result.status_code < 400:
        marmot_json = result.json()
    else:
        print("{} Error getting {}, sleeping 10 seconds".format(result.text,
            url))
        time.sleep(10)
        result = requests.get(url)
        marmot_json = result.json()
    docs = marmot_json['result']['docs']
    total_pages = marmot_json['result']['numPages']
    bf_graph = rdflib.Graph()
    bf_graph.namespace_manager.bind("bf", BF)
    bf_graph.namespace_manager.bind("relators", RELATORS)
    for doc in docs:
        instance_uri = rdflib.URIRef("https://plains2peaks.org/{}".format(
            uuid.uuid1()))
        __generation_process__(instance_uri, bf_graph)
        bf_graph.add((instance_uri, rdflib.RDF.type, BF.Instance))
        work_uri = rdflib.URIRef("{}#Work".format(instance_uri))
        bf_graph.add((work_uri, rdflib.RDF.type, BF.Work))
        bf_graph.add((instance_uri, BF.instanceOf, work_uri))
        item_uri = rdflib.URIRef(doc.get('isShownAt'))
        __generation_process__(item_uri, bf_graph)
        bf_graph.add((item_uri, BF.itemOf, instance_uri))
        bf_graph.add((item_uri, rdflib.RDF.type, BF.Item))
        cover_art = rdflib.BNode()
        bf_graph.add((cover_art, rdflib.RDF.type, BF.CoverArt))
        bf_graph.add((instance_uri, BF.coverArt, cover_art))
        bf_graph.add((cover_art, rdflib.RDF.value,
            rdflib.URIRef(doc.get('preview'))))
        institution = __marmot_orgs__(doc.get('dataProvider'))
        bf_graph.add((item_uri, BF.heldBy, institution))
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
        bf_graph.add((item_uri, BF.usageAndAccessPolicy, rdflib.URIRef(doc.get('rights'))))
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
            bf_graph.add((work_uri, RELATORS.cre, creator))
        extent_str = doc.get('extent','')
        if len(extent_str) > 0:
            extent = rdflib.BNode()
            bf_graph.add((instance_uri, BF.extent, extent))
            bf_graph.add((extent, rdflib.RDF.type, BF.Extent))
            bf_graph.add((extent, rdflib.RDF.value, rdflib.Literal(extent_str)))
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
            collection = rdflib.URIRef("{}marmot-collection/{}".format(
                BASE_URL,
                bibcat.slugify(row)))
            bf_graph.add((collection, rdflib.RDF.type,
                       BF.Collection))
            bf_graph.add((collection, rdflib.RDFS.label,
                       rdflib.Literal(row)))
            bf_graph.add((work_uri, BF.partOf, collection))
    P2P_DEDUPLICATOR.run(bf_graph)
    return bf_graph, int(total_pages)

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
