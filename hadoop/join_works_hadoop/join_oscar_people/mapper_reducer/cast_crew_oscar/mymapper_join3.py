#!/usr/bin/env python3
import logging
import sys
import re
from pprint import pprint

log = logging.getLogger(__name__)

from pymongo_hadoop import BSONMapper


def mapper(documents):
    try:
        #f=open("doc_formatted.txt","a+")
        for doc in documents:
            doc_formatted = {"_id": ""}
            keys = [key for key in doc.keys()]
            keys.remove("_id")
            tmp={}
            person_name = ""
            if "cast_roles" in keys:
                tmp = doc["cast_roles"]
                doc_formatted["coll_name"] = "cast"
                doc_formatted["person_id"] = tmp["person_id"]
                tmp_name = tmp["person_name"].split()
                tmp_name.sort()
                person_name = "-".join(tmp_name)
            elif "crew_roles" in keys:
                tmp = doc["crew_roles"]
                doc_formatted["coll_name"] = "crew"
                doc_formatted["person_id"] = tmp["person_id"]
                tmp_name = tmp["person_name"].split()
                tmp_name.sort()
                person_name = "-".join(tmp_name)
            elif "oscar_nominations" in keys:
                tmp = doc["oscar_nominations"]
                doc_formatted["coll_name"] = "oscar"
                tmp_name = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(tmp["name"]) ).split()]
                #tmp_name = tmp["name"].split()
                tmp_name.sort()
                person_name = "-".join(tmp_name)
                for key in tmp.keys():
                    doc_formatted[key] = tmp[key]
            elif "production_companies" in keys:
                doc_formatted["coll_name"] = "production_companies"
                doc_formatted["company_id"] = doc["production_companies"]
                tmp = doc["company"]
                tmp["movie_id"] = doc["movie_id"]
                tmp_name = [name.lower() for name in re.sub(r'[^\w\s]', ' ', str(tmp["name"])).split()]
                tmp_name.sort()
                person_name = "-".join(tmp_name)

            join_key = str(int(tmp["movie_id"])) + "-" + person_name
            doc_formatted["_id"] = join_key
            #pprint(doc_formatted, stream=f)

            yield doc_formatted
        #f.close()
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)
        #log.exception(e)

BSONMapper(mapper)
