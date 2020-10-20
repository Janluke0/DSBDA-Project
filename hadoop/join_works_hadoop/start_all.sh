#!/bin/bash

echo ADD movie_id to oscar_awards

./pymongo_hadoop_installation.sh
./join_oscar_metadata/collection_refactoring_hdfs.sh
./join_oscar_metadata/join_oscar_metadata.sh

echo ADD person_id to oscar_awards

./join_oscar_people/collection_refactoring_hdfs.sh
./join_oscar_people/join_oscar_people.sh
./join_oscar_people/final.sh
