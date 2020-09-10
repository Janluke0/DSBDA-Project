#!/bin/bash

#remove output direcotry
hadoop fs -rm -r -f  /tmp/out


INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.keywords?readPreference=primary
QUERY={_id:{\$gt:30}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars ./mongo-hadoop-streaming-2.0.2.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D mongo.input.query=$QUERY\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -input /tmp/in\
 -output /tmp/out\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -io mongodb\
 -mapper ./mapper.py\
 -reducer ./reducer.py
# 