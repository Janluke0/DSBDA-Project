#!/usr/bin/env python3
import logging
import sys
log = logging.getLogger(__name__)

from pymongo_hadoop import BSONMapper
def mapper(documents):
    for doc in documents:
        for _ in doc['movie_ids']:
            yield {'_id': doc['_id'], 'count':1}

BSONMapper(mapper)
