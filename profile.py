#!/usr/bin/python3.5
import datetime
import os
import requests

PROJECT_BASE =  os.path.abspath(os.path.dirname(__file__))
TRIPLESTORE_URL = "http://localhost:9999/blazegraph/sparql"   

def load():
    start = datetime.datetime.now()
    print("Loading RDF turtle files for Plains2Peaks  at {}".format(
               start.isoformat()))
    # Load custom ttl files for institutional metadata for richer
    # context for ttl files in the data directory
    headers = {"Content-type": "text/turtle"} 
    for directory in ["data"]: 
        turtle_path = os.path.join(PROJECT_BASE, directory)
        walker = next(os.walk(turtle_path))
        for filename in walker[2]:
            if not filename.endswith("ttl"):
                continue
            full_path = os.path.join(turtle_path, filename)
            with open(full_path, "rb") as fo:
                raw_turtle = fo.read()
            result = requests.post(TRIPLESTORE_URL,
                          data=raw_turtle,
                          headers=headers)
            if result.status_code < 400:
                print("\t{} ingest result {}".format(filename, 
                          result.text))
    end = datetime.datetime.now()
    print("Finished RDF turtle load at {}, total time {} minutes".format(
               end,
               (end-start).seconds / 60.0))

if __name__ ==  '__main__':
    load()
