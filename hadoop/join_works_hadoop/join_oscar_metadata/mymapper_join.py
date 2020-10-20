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
            if "release_year" in keys and "original_title" in keys:
                tmp_list = doc['original_title'].copy()
                tmp_list.sort()
                tmp_list.insert(0, str(doc['release_year']))
                doc_formatted['_id'] = doc_formatted["_id"].join(tmp_list)
                doc_formatted["coll_name"] = "metadata"
                for key in keys:
                    if key == "_id":
                        doc_formatted['old_id'] = doc[key]["_id"]
                    else:
                        doc_formatted[key] = doc[key]
                #print(doc_formatted, file=sys.stderr)
                #pprint("\nMETADATA\n", stream=f)
                #pprint(doc_formatted, stream=f)
            if "year_film" in keys and "film" in keys:
                tmp_list = doc['film'].copy()
                tmp_list.sort()
                tmp_list.insert(0, str(doc["year_film"]))
                doc_formatted['_id'] = doc_formatted["_id"].join(tmp_list)
                doc_formatted["coll_name"] = "oscar_awards"
                for key in keys:
                    if key == "_id":
                        doc_formatted["old_id"] = doc[key]["_id"]
                    else:
                        doc_formatted[key] = doc[key]
                #print(doc_formatted, file= sys.stderr)
                #pprint("\nOSCAR\n", stream=f)
                #pprint(doc_formatted, stream=f)
            yield doc_formatted
        #f.close()
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)
        #log.exception(e)

BSONMapper(mapper)
