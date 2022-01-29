#!/usr/bin/env python
# -*- coding: utf-8 -*-

# COMMON_TODAY_STR = r"""today=`date -d '2 days ago' '+%Y-%m-%d'`;
# """
# COMMON_YESTERDAY_STR = r"""yesterday=`date -d '3 day ago' '+%Y-%m-%d'`;
# """
#
COMMON_TODAY_STR = r"""today=`date -d 'now' '+%Y-%m-%d'`;
"""
COMMON_YESTERDAY_STR = r"""yesterday=`date -d '1 day ago' '+%Y-%m-%d'`;
"""

# sqoop 全量同步模版1,需要check_column
FIRST_FULL_SYNC_SHELL_FORMAT = r"""#!/bin/sh
""" + COMMON_TODAY_STR + COMMON_YESTERDAY_STR + r"""
hive {auth} -e 'drop table {hive_database}.{biz_hive_table}';
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{target_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query "SELECT * FROM {target_database}.{source_table} WHERE {check_column} < '$today' and \$CONDITIONS" \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {source}__{source_database}__{source_table} \
--create-hcatalog-table \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};
if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""

# sqoop 全量同步模版1,不需要check_column
FIRST_FULL_SYNC_SHELL_FORMAT_V2 = r"""#!/bin/sh
""" + COMMON_TODAY_STR + COMMON_YESTERDAY_STR + r"""
hive {auth} -e 'drop table {hive_database}.{biz_hive_table}';
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{target_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query 'SELECT * FROM {target_database}.{source_table} WHERE $CONDITIONS' \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {source}__{source_database}__{source_table} \
--create-hcatalog-table \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};
if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""
# sqoop 全量同步模版2
DAILY_FULL_SYNC_SHELL_FORMAT = r"""#!/bin/sh
history_date=`date -d '4 day ago' '+%Y-%m-%d'`;
thedate_str=`date -d '1 day ago' '+%Y-%m-%d'`;
hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$history_date')";
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query 'SELECT * FROM {source_database}.{source_table} WHERE $CONDITIONS' \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {source}__{source_database}__{source_table} \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};
if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""


# sqoop 增量同步模版
# DAILY_APPEND_SYNC_SHELL_FORMAT = r"""#!/bin/sh
# today=`date -d 'now' '+%Y-%m-%d'`;
# yesterday=`date -d '1 day ago' '+%Y-%m-%d'`;
# hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$yesterday')";
# sqoop import \
# --connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
# --username {username} \
# --password {password} \
# -m 1 \
# --split-by {split_column} \
# --query "SELECT * FROM {source_database}.{source_table} WHERE {check_column} >= '$yesterday' and {check_column} < '$today' and \$CONDITIONS" \
# --hive-drop-import-delims \
# --hcatalog-database biz \
# --hcatalog-table {source}__{source_database}__{source_table} \
# --hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
# --hcatalog-partition-keys thedate \
# --hcatalog-partition-values $yesterday \
# --class-name {source_database}.{source_table};
# if [ $? -eq 0 ]
# then
#     echo 0
# else
#     exit 1
# fi"""

# sqoop 增量同步模版(append改动）
DAILY_APPEND_SYNC_SHELL_FORMAT = r"""#!/bin/sh 
flag=$(python /data/1/Hermes/generators/monitor.py {host} {username} {password} {port} {source_database} {source_table} {source} {env})
if [[ "$flag" == 1 ]];then
    """ + COMMON_TODAY_STR + """    """ + COMMON_YESTERDAY_STR + r"""
    hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$yesterday')";
    sqoop import \
    --connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
    --username {username} \
    --password {password} \
    -m 10 \
    --split-by {split_column} \
    --query "SELECT * FROM {source_database}.{source_table} WHERE {check_column} >= '$yesterday' and {check_column} < '$today' and \$CONDITIONS" \
    --hive-drop-import-delims \
    --hcatalog-database biz \
    --hcatalog-table {source}__{source_database}__{source_table} \
    --hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
    --hcatalog-partition-keys thedate \
    --hcatalog-partition-values $yesterday \
    --class-name {source_database}.{source_table};
    if [ $? -eq 0 ]
    then
        echo 0
    else
        exit 1
    fi
else
    python /data/1/Hermes/generators/hermes_overwrite_mail_monitor.py {source_database} {source_table} {source} {env}
    """ + COMMON_TODAY_STR + """    """ + COMMON_YESTERDAY_STR + r"""
    hive {auth} -e 'drop table {hive_database}.{biz_hive_table}';
    sqoop import \
    --connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
    --username {username} \
    --password {password} \
    -m 10 \
    --split-by {split_column} \
    --query "SELECT * FROM {source_database}.{source_table} WHERE {check_column} < '$today' and \$CONDITIONS" \
    --hive-drop-import-delims \
    --hcatalog-database {hive_database} \
    --hcatalog-table {source}__{source_database}__{source_table} \
    --create-hcatalog-table \
    --hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
    --hcatalog-partition-keys thedate \
    --hcatalog-partition-values $yesterday \
    --class-name {source_database}.{source_table};
    if [ $? -eq 0 ]
    then
        echo 0
    else
        exit 1
    fi
fi"""


### 线上新的增量同步模板
DAILY_APPEND_ONLINE_SYNC_SHELL_FORMAT = r"""#!/bin/sh
""" + COMMON_TODAY_STR + COMMON_YESTERDAY_STR + r"""
sql=$(python  /home/create_hive_sql_manage.py {hive_database} {biz_hive_table} {host} {username} {password} {source_database} {port} {source_table})
hive {auth} -e  "$sql";
hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$yesterday')";
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query "SELECT * FROM {source_database}.{source_table} WHERE {check_column} >= '$yesterday' and {check_column} < '$today' and \$CONDITIONS" \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {biz_hive_table} \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};

if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""

### 线上新的全量的同步模板(包括快照模式 有check_column）
DAILY_SNAPSHOT_ONLINE_SYNC_SHELL_FORMAT_V1 = r"""#!/bin/sh
""" + COMMON_TODAY_STR + COMMON_YESTERDAY_STR + r"""
sql=$(python  /home/create_hive_sql_manage.py {hive_database} {biz_hive_table} {host} {username} {password} {source_database} {port} {source_table})
hive {auth} -e  "$sql";
hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$yesterday')";
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query "SELECT * FROM {source_database}.{source_table} WHERE {check_column} < '$today' and \$CONDITIONS" \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {biz_hive_table} \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};

if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""

### 线上新的全量的同步模板(包括快照模式 无check_column）
DAILY_SNAPSHOT_ONLINE_SYNC_SHELL_FORMAT_V2 = r"""#!/bin/sh
""" + COMMON_TODAY_STR + COMMON_YESTERDAY_STR + r"""
sql=$(python  /home/create_hive_sql_manage.py {hive_database} {biz_hive_table} {host} {username} {password} {source_database} {port} {source_table})
hive {auth} -e  "$sql";
hive {auth} -e "alter table {hive_database}.{biz_hive_table} drop if exists partition(thedate='$yesterday')";
sqoop import \
--connect 'jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false' \
--username {username} \
--password {password} \
-m 1 \
--split-by {split_column} \
--query "SELECT * FROM {source_database}.{source_table} WHERE \$CONDITIONS" \
--hive-drop-import-delims \
--hcatalog-database {hive_database} \
--hcatalog-table {biz_hive_table} \
--hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
--hcatalog-partition-keys thedate \
--hcatalog-partition-values $yesterday \
--class-name {source_database}.{source_table};

if [ $? -eq 0 ]
then
    echo 0
else
    exit 1
fi"""

# azkaban job模版
AZKABAN_JOB_FORMAT = r"""# {job_name}.job
type=command
retries={retries}
command={command}
dependencies={dependencies}"""


SPARK_JOB_FORMAT = r"""bash -c "source activate py35 && su - zeppelin -c \
'/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.broadcastTimeout=2000 \
--num-executors 12 \
--executor-memory 4g \
--executor-cores 2 \
--conf spark.hljEnv=offline \
--jars /usr/hdp/2.6.3.0-235/phoenix/lib/phoenix-spark2-4.7.0.2.6.3.0-235.jar,\
/usr/hdp/2.6.3.0-235/phoenix/phoenix-4.7.0.2.6.3.0-235-client.jar \
{her_base_path}apps/etl_jobs/{database}/{job_name}.py' && source deactivate" """

SPARK_JOB_FORMAT_ONLINE = r"""bash -c "source activate py35 && sudo su - zeppelin -c \
'/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--driver-memory 2g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.broadcastTimeout=2000 \
--num-executors 10 \
--executor-memory 4g \
--executor-cores 2 \
--conf spark.hljEnv=online \
--conf spark.sql.autoBroadcastJoinThreshold=5857600 \
--conf spark.sql.broadcastTimeout=2000 \
--jars /usr/hdp/3.0.0.0-1634/phoenix/lib/phoenix-spark-5.0.0.3.0.0.0-1634.jar,\
/usr/hdp/3.0.0.0-1634/phoenix/phoenix-5.0.0.3.0.0.0-1634-client.jar \
{her_base_path}apps/etl_jobs/{database}/{job_name}.py' && source deactivate" """


CLEAR_YARN_ZEPPELIN_JOBS = r"""
#!/usr/bin/python
# encode=utf-8
import requests
posturl = "http://ai.hljnbw.cn/s/api/login"
d = {
    "userName": "admin",
    "password": "12adjj33!@"
}
session = requests.Session()
r = session.post(posturl, data=d)
print(dict(r.cookies))
# get spark inter
nexturl = "http://ai.hljnbw.cn/s/api/interpreter/setting"
r = session.get(nexturl)
interpreter = ""
for _ in r.json()["body"]:
    if _["group"] == "spark":
        interpreter = _["id"]
print(interpreter)
restart_url = "http://ai.hljnbw.cn/s/api/interpreter/setting/restart/%s"%interpreter
r = session.put(restart_url)
print(r.json()["status"])
"""