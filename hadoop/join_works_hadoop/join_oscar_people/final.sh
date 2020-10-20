mongo movie_dataset --eval 'db.oscar_awards_complete_out.drop()'

OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.oscar_awards_complete_out

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mongo.output.uri=$OUTPUT_URI\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input  /tmp/out/movie_dataset\
 -output /tmp/join3\
 -inputformat com.mongodb.hadoop.mapred.BSONFileInputFormat\
 -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/cast_crew_oscar/mymapper_join3.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/cast_crew_oscar/myreducer_join3.py


####################################################################

mongo movie_dataset --eval 'db.oscar_awards_complete2_out.drop()'

hadoop fs -rm -r -f  /tmp/out/movie_dataset_2
echo ## Export in HadoopFileSystem the previous results
mongodump --db=movie_dataset --collection=oscar_awards_complete_out --out=./
mongodump --db=movie_dataset --collection=production_companies --out=./
rm -r ./movie_dataset/*.json
mv movie_dataset movie_dataset_2
hadoop fs -copyFromLocal ./movie_dataset_2 hdfs:///tmp/out
rm -r ./movie_dataset_2

OUTPUT_URI=mongodb://127.0.0.1:27017/movie_dataset.oscar_awards_complete2_out

hadoop jar $HADOOP_INSTALL/share/hadoop/tools/lib/hadoop-streaming* \
 -libjars /home/smaike/join_works_hadoop/libjars/mongo-hadoop-streaming-2.0.2_mod.jar\
 -D stream.io.identifier.resolver.class=com.mongodb.hadoop.streaming.io.MongoIdentifierResolver\
 -D mongo.output.uri=$OUTPUT_URI\
 -D mapred.output.committer.class=com.mongodb.hadoop.mapred.output.MongoOutputCommitter\
 -input  /tmp/out/people\
 -input /tmp/out/movie_dataset_2\
 -output /tmp/join4\
 -inputformat com.mongodb.hadoop.mapred.BSONFileInputFormat\
 -outputformat com.mongodb.hadoop.mapred.MongoOutputFormat\
 -io mongodb\
 -mapper ~/join_works_hadoop/join_oscar_people/mapper_reducer/final/mymapper_join4.py\
 -reducer ~/join_works_hadoop/join_oscar_people/mapper_reducer/final/myreducer_join4.py


mongo movie_dataset --eval 'db.oscar_awards_complete_out.aggregate([{$unionWith: {"coll": "oscar_awards_complete2_out"}},{$out:{db:"movie_dataset",coll:"oscar_awards_final_out"}}])'

mongo movie_dataset --eval 'db.oscar_awards_final_out.remove({person_id:{$eq:null}})'

mongo movie_dataset --eval 'db.oscar_awards_complete2_out.drop()'
mongo movie_dataset --eval 'db.oscar_awards_complete_out.drop()'
