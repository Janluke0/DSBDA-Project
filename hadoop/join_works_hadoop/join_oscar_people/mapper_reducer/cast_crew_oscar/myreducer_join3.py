#!/usr/bin/env python3
import logging
import sys
from pprint import pprint
from bson.objectid import ObjectId

log = logging.getLogger(__name__)

from pymongo_hadoop import BSONReducer, BSONReducerInput




def reducer(key, values):
    try:
        #f=open("tmp_result_join.txt","a+")
        #pprint("-KEY-", stream=f)
        #pprint(key, stream=f)
        obj_id = ObjectId()
        rst_dict = {"_id" : obj_id, "person_id" : None, "company": None}
        for v in values:
            #pprint("-VALUE-", stream=f)
            #pprint(v, stream=f)
            if v["coll_name"] == "cast" or v["coll_name"] == "crew":
                rst_dict["person_id"] = v["person_id"]
                rst_dict["company"] = False
            elif v["coll_name"] == "production_companies":
                rst_dict["person_id"] = v["company_id"]
                rst_dict["company"] = True
            elif v["coll_name"] == "oscar":
                keys = [key for key in v.keys()]
                keys.remove("_id")
                for key in keys:
                    rst_dict[key] = v[key]
        #f.close()
        if len(rst_dict.keys()) == 3:
            return None
        else:
            return rst_dict
    except Exception as e:
        print ("End Reducer\n" + str(e), file=sys.stderr)

BSONReducer(reducer)

