#!/bin/bash

hadoop fs -rm -r -f  /tmp/join2
mongo movie_dataset --eval 'db.cast_crew_people_join_out.drop()'
#hadoop fs -rm -r -f  /tmp/out/metadata
#hadoop fs -rm -r -f  /tmp/out/people


#####
echo ---CAST, CREW, PEOPLE JOIN - OUTPUT on MongoDb ---
echo  ##STEP1## Add person name to cast and crew collections

#INPUT_URI_CAST=mongodb://127.0.0.1:27017/movie_dataset.cast
#INPUT_URI_CREW=mongodb://127.0.0.1:27017/movie_dataset.crew
#INPUT_URI_PEOPLE=mongodb://127.0.0.1:27017/movie_dataset.people
OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.cast_crew_people_join_out
#QUERY_OUT={movie_id:{\$ne:null}}
#QUERY={\$lookup:{from:movie_dataset.cast,localField:_id,foreignField:movie_id,as:_id}}

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mongo.output.uri=$OUTPUT_URI\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input  /tmp/out/cast\
 -input  /tmp/out/crew\
 -input  /tmp/out/people\
 -output /tmp/join2/cast_crew_people\
 -inputformat com.mongodb.hadoop.mapred.BSONFileInputFormat\
 -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/cast_crew_people/mymapper_join2.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/cast_crew_people/myreducer_join2.py

#rm part-00000-r-00000.bson
#hadoop dfs -get /tmp/join/metadata_oscar/part-00000-r-00000.bson
#mongorestore -d movie_dataset -c oscar_join_def_out part-00000-r-00000.bson

#mongo movie_dataset --eval db.oscar_metadata_join_out.remove({movie_id:{$eq:null}})
mongo movie_dataset --eval 'db.cast_crew_people_join_out.remove({name:{$eq:null}})'

mongo movie_dataset --eval 'db.cast_roles_out.drop()'
mongo movie_dataset --eval 'db.crew_roles_out.drop()'
mongo movie_dataset --eval 'db.metadata_companies_out.drop()'
mongo movie_dataset --eval 'db.production_companies_out.drop()'

echo ## Take crew person name and ID
mongo movie_dataset --eval 'db.cast_crew_people_join_out.aggregate([{$unwind:{path:"$crew_roles"}},{$set:{"_id":"$crew_roles._id"}},{$project:{"crew_roles.person_id": 1,"crew_roles.movie_id": 1,"crew_roles.person_name": 1,"crew_roles.department": 1}},{$out:{db:"movie_dataset",coll:"crew_roles_out"}}])'

echo ## Take cast person name and ID
mongo movie_dataset --eval 'db.cast_crew_people_join_out.aggregate([{$unwind:{path:"$cast_roles"}},{$set:{"_id":"$cast_roles._id"}},{$project:{"cast_roles.person_id": 1,"cast_roles.movie_id": 1,"cast_roles.person_name": 1,"cast_roles.department": 1}},{$out:{db:"movie_dataset",coll:"cast_roles_out"}}])'

echo ## Explode and join metadata,production companies collection
mongo movie_dataset --eval 'db.metadata.aggregate([{$addFields:{"movie_id":"$_id"}},{$unwind:{path:"$production_companies"}},{$set:{"_id": {$concat:[{$toString:"$movie_id"},"-",{$toString:"$production_companies"}]}}},{$out:{db:"movie_dataset",coll:"metadata_companies_out"}}])'

mongo movie_dataset --eval 'db.metadata_companies_out.aggregate([{$lookup:{from: "production_companies",localField: "production_companies",foreignField: "_id",as: "company"}},{$unwind:{path:"$company"}},{$project:{"_id": 1, "movie_id": 1, "production_companies": 1, "company.name":1}},{$out:{db:"movie_dataset",coll:"production_companies_out"}}])'

hadoop fs -rm -r -f  /tmp/out/movie_dataset
echo ## Export in HadoopFileSystem the previous results
mongodump --db=movie_dataset --collection=crew_roles_out --out=./
mongodump --db=movie_dataset --collection=cast_roles_out --out=./
mongodump --db=movie_dataset --collection=oscar_nominations_out --out=./
mongodump --db=movie_dataset --collection=production_companies_out --out=./
rm -r ./movie_dataset/*.json
hadoop fs -copyFromLocal ./movie_dataset hdfs:///tmp/out
rm -r ./movie_dataset

##################################################################################################################################################

echo ---STOP---

