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
        rst_dict = {"_id" : obj_id, "person_id" : key ,"name" : None , "cast_roles" : [] ,"crew_roles": []}
        for v in values:
            #pprint("-VALUE-", stream=f)
            #pprint(v, stream=f)
            if v["coll_name"] == "cast":
                v["person_id"] = int(v["person_id"])
                rst_dict["cast_roles"].append(v)
            elif v["coll_name"] == "crew":
                v["person_id"] = int(v["person_id"])
                rst_dict["crew_roles"].append(v)
            elif v["coll_name"] == "people":
                rst_dict["name"] = v["name"]
        if rst_dict["name"] is not None:
            person_name = rst_dict["name"]
            for cast_role in rst_dict["cast_roles"]:
                cast_role["_id"] = ObjectId()
                cast_role["person_name"] = person_name
            for crew_role in rst_dict["crew_roles"]:
                crew_role["_id"] = ObjectId()
                crew_role["person_name"] = person_name
        #f.close()
        return rst_dict
    except Exception as e:
        print ("End Reducer\n" + str(e), file=sys.stderr)

BSONReducer(reducer)

