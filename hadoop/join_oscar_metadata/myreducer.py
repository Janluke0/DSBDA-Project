#!/usr/bin/env python3
import logging
import sys
log = logging.getLogger(__name__)

from pymongo_hadoop import BSONReducer, BSONReducerInput




def reducer(key, values):
    try:
        tmp_dict = {'_id':key}
        for v in values:
            for key in v.keys():
                tmp_dict[key] =  v[key]
        return tmp_dict
    except:
        print ("End Reducer", file=sys.stderr)

BSONReducer(reducer)

