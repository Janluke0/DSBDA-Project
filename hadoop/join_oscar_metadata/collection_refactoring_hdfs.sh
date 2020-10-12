#!/bin/bash

hadoop fs -rm -r -f  /tmp/out/oscar_awards
hadoop fs -rm -r -f  /tmp/out/metadata
hadoop fs -rm -r -f  /tmp/out/people


#####
echo ---OSCAR AWARDS on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.oscar_awards?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars libjars/mongo-hadoop-streaming-2.0.2.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/oscar_awards\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper mymapper.py\
 -reducer myreducer.py

echo ---STOP---


echo ---METADATA on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars libjars/mongo-hadoop-streaming-2.0.2.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/metadata\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper mymapper.py\
 -reducer myreducer.py

echo ---STOP---

echo ---PEOPLE on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.people?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars libjars/mongo-hadoop-streaming-2.0.2.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/people\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper mymapper.py\
 -reducer myreducer.py

echo ---STOP---

