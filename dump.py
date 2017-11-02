__author__ = "Jeremy Nelson"

import datetime
import rdflib

import bibcat.rml.processor as processor
from zipfile import ZipFile, ZIP_DEFLATED
from resync import Resource, ResourceDumpManifest
from SPARQLWrapper import SPARQLWrapper, JSON

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
MAPv4_context = {"edm": "http://www.europeana.eu/schemas/edm/",
		 "dcterms": "http://purl.org/dc/terms/",
		 "org": "http://www.openarchives.org/ore/terms"}
W3C_DATE = "%Y-%m-%dT%H:%M:%SZ"

def process_turtle(
        filepath, 
        manifest,
        rml_rules, 
        zip_output):
    start = datetime.datetime.utcnow()
    filename = filepath.split("/")[-1]
    msg = "Started processing {} at {}".format(filename, start)
    print(msg)
    graph = rdflib.Graph()
    graph.parse(filepath, format='turtle')
    dpla_mapv4 = processor.SPARQLBatchProcessor(
        triplestore=graph,
        rml_rules=rml_rules)
    count = 0
    for item in graph.subjects(
        predicate=rdflib.RDF.type,
        object=BF.Item):
        if not count%25 and count > 0:
            msg = "."
            print(msg, end="")
        if not count%100:
            msg = "{:,}".format(count)
            print(msg, end="")
        count += 1
        instance = graph.value(subject=item,
                               predicate=BF.itemOf)
        key = str(instance).split("/")[-1]
        path = "/resources/{}.json".format(key)
        dpla_mapv4.run(instance_iri=str(instance),
                       item_iri=str(item))
        raw_json = dpla_mapv4.output.serialize(
                                    format='json-ld',
                                    context=MAPv4_context)
        zip_output.writestr(path, raw_json)
        mod_date = None
        gen_process = graph.value(subject=instance,
                                  predicate=BF.generationProcess)
        
        if gen_process is not None:
            mod_date = graph.value(subject=gen_process,
                                   predicate=BF.generationDate)
        if mod_date is None:
            mod_date = datetime.datetime.utcnow().strftime(W3C_DATE)
        manifest.add(
            Resource(str(instance),
                     lastmod=mod_date,
                     length="{}".format(len(raw_json)),
                     path=path))

    end = datetime.datetime.utcnow()
    msg = "Finished at {}, total time {} for {:,} instances".format(
        end,
        (end-start).seconds / 60.0,
        count)
    print(msg)
    return count

def create_dump_file(
    turtle_files,
    rules,
    zipfile_path):
    """Creates a ZIP file with all of """
    start = datetime.datetime.utcnow()
    print("Starting MAPv4 Dump process at {}".format(start))
    manifest = ResourceDumpManifest()
    rml_rules = ["bf-to-map4.ttl"] + rules
    zipfile_file = "{}/dump.zip".format(zipfile_path)
    zip_output = ZipFile(zipfile_file,
        mode='w',
        compression=ZIP_DEFLATED,
        allowZip64=True)
    total = 0
    for ttl_file in turtle_files:
        total += process_turtle(ttl_file,
                    manifest,
                    rml_rules,
                    zip_output)
    zip_output.writestr("manifest.xml", manifest.as_xml())
    zip_output.close()
    print("Finished all at {}")
