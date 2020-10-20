#!/usr/bin/env python3
import logging
import sys
import re
from pprint import pprint

log = logging.getLogger(__name__)

from pymongo_hadoop import BSONMapper


def mapper(documents):
    try:
        #print(documents, file=sys.stderr)
        #f=open("doc_formatted.txt","a+")
        for doc in documents:
            doc_formatted = {"_id": ""}
            keys = [key for key in doc.keys()]
            keys.remove("_id")
            collection_name = doc["coll_name"]

            if collection_name == "people":
                doc_formatted["_id"] = int(doc["_id"]["_id"])
            elif collection_name == "cast" or collection_name == "crew" :
                doc_formatted["_id"] = int(doc["person_id"])

            for key in keys:
                doc_formatted[key] = doc[key]
            yield doc_formatted
        #f.close()
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)
        #log.exception(e)

BSONMapper(mapper)
