#!/bin/sh

BASEDIR=$(dirname $0)
ROOTDIR=$(cd ${BASEDIR}/..; pwd)
NOSQL_UTIL_JAR="${ROOTDIR}/build/libs/nosql-util-0.0.1.jar"

CLASSPATH="$(hadoop classpath):$(hbase classpath):${NOSQL_UTIL_JAR}"

HADOOP_CLASSPATH=${CLASSPATH} HBASE_CLASSPATH=${CLASSPATH} \
  hbase com.nuaavee.nosql.Driver export entity details_v2 "demo:8:" "demo:9" csv

hdfs dfs -text /tmp/exported/entity/demo_8/part-m-00000
