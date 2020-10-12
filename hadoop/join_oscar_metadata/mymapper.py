#!/usr/bin/env python3
import logging
import sys
import re

log = logging.getLogger(__name__)

from pymongo_hadoop import BSONMapper


def mapper(documents):
    try:
        for doc in documents:
            name_splitted = []
            film_name_splitted = []
            doc_formatted = {'_id':doc['_id']}
            for key in doc.keys():
                if key == "name":
                    name_splitted = [ name.lower() for name in re.sub(r'[^\w\s]', ' ', str(doc[key])).split()]
                    #name_splitted.sort()
                    #name_spliited = [name  for name in name_splitted if len(name)>1]
                    doc_formatted[key] = name_splitted
                elif key == "film" or key == "original_title":
                    film_name_splitted = [ film_name.lower() for film_name in re.sub(r'[^\w\s]', ' ', str(doc[key])).split()]
                    #film_name_splitted.sort()
                    doc_formatted[key] = film_name_splitted
                elif key == "release_date":
                    tmp_list=[num for num in str(doc[key]).split("-") if len(num) == 4]
                    if len(tmp_list) > 0:
                       doc_formatted["release_year"]=tmp_list[0]
                    doc_formatted[key] = doc[key]
                else:
                    doc_formatted[key] = doc[key]
            yield doc_formatted
    except Exception as e:
        print("End Mapper\n" + str(e), file=sys.stderr)

BSONMapper(mapper)
