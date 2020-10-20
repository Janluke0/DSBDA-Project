#!/usr/bin/env python3
import logging
import sys
import re
from pprint import pprint

log = logging.getLogger(__name__)

from pymongo_hadoop import BSONMapper
#tmp_name = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(tmp["name"])).split()]

def mapper(documents):
    try:
        #f=open("doc_formatted.txt","a+")
        for doc in documents:
            doc_formatted = {"_id": ""}
            keys = [key for key in doc.keys()]
            keys.remove("_id")
            tmp={}
            person_name = ""
            collection_name = None
            if "coll_name" in keys:
                if doc["coll_name"] == "oscar" and doc["person_id"] is None:
                    tmp_name = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(doc["name"])).split()]
                    tmp_name.sort()
                    person_name = "-".join(tmp_name)
                    for key in keys:
                        doc_formatted[key] = doc[key]
                    doc_formatted["_id"] = person_name
                    #pprint(doc_formatted, stream=f)
                    yield doc_formatted
                elif doc["coll_name"] == "people":
                    tmp_name = doc["name"].split()
                    tmp_name.sort()
                    person_name= "-".join(tmp_name)
                    doc_formatted["person_id"] = int(doc["_id"]["_id"])
                    doc_formatted["coll_name"] = doc["coll_name"]
                    doc_formatted["_id"] = person_name
                    #pprint(doc_formatted, stream=f)
                    yield doc_formatted
            else:
                tmp_name = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(doc["name"])).split()]
                tmp_name.sort()
                company_name = "-".join(tmp_name)
                doc_formatted["person_id"] = int(doc["_id"])
                doc_formatted["coll_name"] = "production_companies"
                doc_formatted["_id"] = company_name
                #pprint(doc_formatted, stream=f)
                yield doc_formatted

            #yield doc_formatted
        #f.close()
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)
        #log.exception(e)

BSONMapper(mapper)
