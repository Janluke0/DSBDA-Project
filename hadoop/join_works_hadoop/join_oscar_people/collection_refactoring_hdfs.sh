#!/bin/bash

hadoop fs -rm -r -f  /tmp/out/cast
hadoop fs -rm -r -f  /tmp/out/crew
hadoop fs -rm -r -f  /tmp/out/people


#####
echo ---CAST on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.cast?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/cast\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/mymapper2.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/myreducer2.py

echo ---STOP---


echo ---CREW on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.crew?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/crew\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/mymapper2.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/myreducer2.py

echo ---STOP---

echo ---PEOPLE on HDFS bson format---
INPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.people?readPreference=primary
#OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.metadata_out
#QUERY_OUT={_id:_id,movies:count}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D mongo.input.uri=$INPUT_URI\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input /tmp/in\
 -output /tmp/out/people\
 -inputformat com.mongodb.hadoop.mapred.MongoInputFormat\
 -outputformat com.mongodb.hadoop.mapred.BSONFileOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/mymapper2.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/refactoring/myreducer2.py

echo ---STOP---

