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
            doc_formatted = {"_id": doc["_id"]}
            keys = [key for key in doc.keys()]
            keys.remove("_id")
            if "gender" in keys:
                #doc_formatted["_id"] = doc["_id"]
                name_splitted = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(doc["name"]) ).split()]
                name_splitted.sort()
                doc_formatted["name"] = " ".join(name_splitted)
                doc_formatted["coll_name"] = "people"
                for key in keys:
                    if key == "jobs":
                        doc_formatted[key] = [str(job).lower() for job in doc[key]]
                    elif key == "name":
                        continue
                    else:
                        doc_formatted[key] = doc[key]
                #print(doc_formatted, file=sys.stderr)
                #pprint("\nPEOPLE\n", stream=f)
                #pprint(doc_formatted, stream=f)
            if "person_id" in keys:
                #doc_formatted["_id"] = doc["person_id"]
                if "cast_id" in keys:
                    doc_formatted["coll_name"] = "cast"
                    #pprint("\nCAST\n", stream=f)
                else:
                    doc_formatted["coll_name"] = "crew"
                    #pprint("\nCREW\n", stream=f)

                for key in keys:
                        doc_formatted[key] = doc[key]
                #print(doc_formatted, file= sys.stderr)
                #pprint(doc_formatted, stream=f)
            yield doc_formatted
        #f.close()
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)
        #log.exception(e)

BSONMapper(mapper)
