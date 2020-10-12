#!/bin/bash

pip install pymongo_hadoop

rm pymongo_hadoop_info.txt
rm pymongo_hadoop_installation_path.txt

pip show pymongo-hadoop >> pymongo_hadoop_info.txt
grep -n "Location" pymongo_hadoop_info.txt  >> pymongo_hadoop_installation_path.txt
installation_path=$(cat pymongo_hadoop_installation_path.txt |  awk  '{print $2}')
module_name="/pymongo_hadoop"
installation_path=$installation_path$module_name

rm pymongo_hadoop_info.txt
rm pymongo_hadoop_installation_path.txt

echo $installation_path

init_module_name="/__init__.py"
init_module_path=$installation_path$init_module_name

input_module_name="/input.py"
input_module_path=$installation_path$input_module_name

output_module_name="/output.py"
output_module_path=$installation_path$output_module_name

mapper_module_name="/mapper.py"
mapper_module_path=$installation_path$mapper_module_name

reducer_module_name="/reducer.py"
reducer_module_path=$installation_path$reducer_module_name

cat ./pymongo_hadoop_lib/__init__.py > $init_module_path
cat ./pymongo_hadoop_lib/input.py > $input_module_path
cat ./pymongo_hadoop_lib/output.py > $output_module_path
cat ./pymongo_hadoop_lib/mapper.py > $mapper_module_path
cat ./pymongo_hadoop_lib/reducer.py > $reducer_module_path
