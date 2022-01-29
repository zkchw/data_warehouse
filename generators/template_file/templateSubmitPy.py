#!/usr/bin/env python
# -*- coding: utf-8 -*-

FORMAT = r"""#!/bin/bash
source /etc/profile
job_name="{job_name}"
file_path_temp="{file_path_temp}"
database="{database}"
driver_memory="{driver_memory}"
num_executors="{num_executors}"
executor_memory="{executor_memory}"
executor_cores="{executor_cores}"
deploy_mode="{deploy_mode}"


"""

OFFLINE_ORIGIN = r"""
/opt/spark/current2/bin/spark-submit \
--master yarn \
--deploy-mode ${deploy_mode} \
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
${file_path_temp}

"""
ONLINE_ORIGIN = r"""
source activate base
/usr/hdp/current/spark2-client/bin/spark-submit \
--jars /data/1/jars/mysql-connector-java-8.0.17.jar \
--master yarn \
--deploy-mode ${deploy_mode} \
--num-executors ${num_executors} \
--executor-memory ${executor_memory} \
--executor-cores ${executor_cores} \
--driver-memory ${driver_memory} \
--conf spark.yarn.executor.memoryOverhead=1G \
--name "${job_name}" \
--conf spark.hljEnv=online \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.port.maxRetries=100 \
--conf spark.sql.broadcastTimeout=2000 \
--conf spark.sql.parquet.writeLegacyFormat=true \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.minNumPostShufflePartitions=3 \
--conf spark.sql.adaptive.maxNumPostShufflePartitions=300 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728 \
--conf spark.sql.adaptive.shuffle.targetPostShuffleRowCount=1000000 \
--conf spark.sql.autoBroadcastJoinThreshold=5857600 \
--conf spark.sql.broadcastTimeout=2000 \
${file_path_temp}
source deactivate
"""