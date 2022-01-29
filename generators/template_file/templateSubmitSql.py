#!/usr/bin/env python
# -*- coding: utf-8 -*-

FORMAT = r"""#!/bin/bash
source /etc/profile
sql="{sql}"
job_name="{job_name}"
driver_memory="{driver_memory}"
num_executors="{num_executors}"
executor_memory="{executor_memory}"
executor_cores="{executor_cores}"

"""

OFFLINE_ORIGIN = r"""
echo "${sql}"
echo "${hive_table}"
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$3" ] ;then
  thedate=$3
else
  thedate=`date -d "-1 days "  "+%Y-%m-%d"`
fi
echo "${thedate}"

finalsql=${sql/\{thedate\}/${thedate}}
echo "${finalsql}"

/opt/spark/current2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--num-executors ${num_executors} \
--executor-memory ${executor_memory} \
--executor-cores ${executor_cores} \
--driver-memory ${driver_memory} \
--conf spark.yarn.executor.memoryOverhead=1G \
--name "${job_name}" \
--conf spark.hljEnv=offline \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.broadcastTimeout=2000 \
--conf spark.sql.parquet.writeLegacyFormat=true \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.minNumPostShufflePartitions=3 \
--conf spark.sql.adaptive.maxNumPostShufflePartitions=300 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleRowCount=1000000 \
--conf spark.sql="${finalsql}" \
--class com.hlj.spark.sparksql /data/spark/sparkjob/hlj-spark-2.4-1.0-SNAPSHOT-jar-with-dependencies.jar offline

"""
ONLINE_ORIGIN = r"""
echo "${sql}"
echo "${hive_table}"
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$3" ] ;then
  thedate=$3
else
  thedate=`date -d "-1 days "  "+%Y-%m-%d"`
fi
echo "${thedate}"

finalsql=${sql//\{thedate\}/${thedate}}
echo "${finalsql}"

source activate base
/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--deploy-mode client \
--num-executors ${num_executors} \
--executor-memory ${executor_memory} \
--executor-cores ${executor_cores} \
--driver-memory ${driver_memory} \
--conf spark.yarn.executor.memoryOverhead=1G \
--name "${job_name}" \
--conf spark.hljEnv=online \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.broadcastTimeout=2000 \
--conf spark.sql.parquet.writeLegacyFormat=true \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.minNumPostShufflePartitions=3 \
--conf spark.sql.adaptive.maxNumPostShufflePartitions=300 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleRowCount=1000000 \
--conf spark.sql="${finalsql}" \
--class com.hlj.spark.sparksql /data/spark/sparkjob/hlj-spark-2.3-1.0-SNAPSHOT-jar-with-dependencies.jar online
source deactivate

"""