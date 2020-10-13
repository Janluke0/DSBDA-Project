#!/bin/bash

hadoop fs -rm -r -f  /tmp/join
mongo movie_dataset --eval 'db.oscar_metadata_join_out.drop()'
#hadoop fs -rm -r -f  /tmp/out/metadata
#hadoop fs -rm -r -f  /tmp/out/people


#####
echo ---OSCAR METADATA JOIN - OUTPUT on MongoDb ---

#INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.oscar_awards?readPreference=primary
OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.oscar_metadata_join_out
#QUERY_OUT={movie_id:{\$ne:null}}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mongo.output.uri=$OUTPUT_URI\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/out/metadata\
 -input /tmp/out/oscar_awards\
 -output /tmp/join/metadata_oscar\
 -inputformat com.mongodb.hadoop.mapred.BSONFileInputFormat\
 -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat\
 -io mongodb\
 -mapper mymapper_join.py\
 -reducer myreducer_join.py

#rm part-00000-r-00000.bson
#hadoop dfs -get /tmp/join/metadata_oscar/part-00000-r-00000.bson
#mongorestore -d movie_dataset -c oscar_join_def_out part-00000-r-00000.bson

mongo movie_dataset --eval 'db.oscar_metadata_join_out.remove({movie_id:{$eq:null}})'

echo ---STOP---

