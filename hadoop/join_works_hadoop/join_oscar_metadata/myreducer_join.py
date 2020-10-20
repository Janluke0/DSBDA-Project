#!/usr/bin/env python3
import logging
import sys
from pprint import pprint
from bson.objectid import ObjectId
log = logging.getLogger(__name__)

from pymongo_hadoop import BSONReducer, BSONReducerInput




def reducer(key, values):
    try:
        #f=open("tmp_result3_join.txt","a+")
        #pprint("-KEY-", stream=f)
        #pprint(key, stream=f)
        obj_id = ObjectId()
        rst_dict = {"_id" : obj_id, "movie_id" : None ,"oscar_nominations": []}
        tmp_movie_id = None
        for v in values:
            #pprint("-VALUE-", stream=f)
            if v["coll_name"] == "metadata":
                tmp_movie_id = v["old_id"]
            if v["coll_name"] == "oscar_awards":
                rst_dict["movie_id"] = tmp_movie_id
                tmp_dict = v.copy()
                tmp_dict.pop("old_id")
                tmp_dict.pop("coll_name")
                oscar_awards_exploded = []
                if len(tmp_dict["name"]) > 1:
                    for name in tmp_dict["name"]:
                        tmp_dict2 = tmp_dict.copy()
                        tmp_dict2["_id"] = ObjectId()
                        tmp_dict2["name"] = name
                        tmp_dict2["movie_id"] = tmp_movie_id
                        oscar_awards_exploded.append(tmp_dict2)
                elif len(tmp_dict["name"]) == 1:
                    tmp_dict["_id"] = ObjectId()
                    tmp_dict["name"] = tmp_dict["name"][0]
                    tmp_dict["movie_id"] = tmp_movie_id
                    oscar_awards_exploded.append(tmp_dict)
                for el in oscar_awards_exploded:
                    rst_dict["oscar_nominations"].append(el)
        return rst_dict
    except Exception as e:
        print ("End Reducer\n" + str(e), file=sys.stderr)

BSONReducer(reducer)

