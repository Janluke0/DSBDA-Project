#!/usr/bin/env python3
import logging
import sys
from pprint import pprint
log = logging.getLogger(__name__)

from pymongo_hadoop import BSONReducer, BSONReducerInput




def reducer(key, values):
    try:
        #f=open("tmp_result3_join.txt","a+")
        #pprint("-KEY-", stream=f)
        #pprint(key, stream=f)
        rst_dict = {"_id" : key, "movie_id" : None ,"oscar_nominations": []}
        tmp_movie_id = None
        for v in values:
            #pprint("-VALUE-", stream=f)
            if v["coll_name"] == "metadata":
                tmp_movie_id = v["old_id"]
            if v["coll_name"] == "oscar_awards":
                tmp_dict = v.copy()
                rst_dict["movie_id"] = tmp_movie_id
                tmp_dict["movie_id"] = tmp_movie_id
                tmp_dict["_id"] = tmp_dict["old_id"]
                tmp_dict.pop("old_id")
                tmp_dict.pop("coll_name")
                rst_dict["oscar_nominations"].append(tmp_dict)
        if rst_dict["movie_id"] is None:
            rst_dict["_id"] = rst_dict["_id"] + "null"
        if rst_dict["movie_id"] is not None:
            rst_dict["_id"] = rst_dict["movie_id"]
        return rst_dict
    except Exception as e:
        print ("End Reducer\n" + str(e), file=sys.stderr)

BSONReducer(reducer)

