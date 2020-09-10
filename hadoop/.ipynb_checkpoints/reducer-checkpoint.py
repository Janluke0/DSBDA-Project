#!/usr/bin/env python3
import logging
import sys
log = logging.getLogger(__name__)

from pymongo_hadoop import BSONReducer, BSONReducerInput




def reducer(key, values):
    _count = 0
    for v in values:
        _count += v['count']
    return {'_id': key, 'count': _count}

BSONReducer(reducer)


exit(0)
#((key, (v for v in values)) for key, values in data)
class CustomReducer(BSONReducer):
    def __init__(self, *args, **kargs):
        self.output = None
        super().__init__(args,kargs)
        
def reducer(gen):
    for k, values in gen:
        c = len([0 for _ in values])
        #log.warn(key,c)
        yield k, c

for el in BSONReducerInput(reducer):
    #print(k,v)
    print(",".join([str(e) for e in el]))