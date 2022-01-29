#!/usr/bin/env python
# -*- coding: utf-8 -*-

OVERWRITE_FORMAT = r"""#!/bin/bash
export HADOOP_USER_NAME=hdfs
#export HADOOP_CONF_DIR=/usr/hdp/3.0.0.0-1634/hadoop/conf

#需要修改开始
mysql_table_name="{source_table}"

columns="
   {columns}
"
create_hivetable_columns="
    {create_hivetable_columns}
"

hive_table="{hive_database}.{biz_hive_table}"
hive_hdfs_path="/hive_data/ods_data/{hive_database}/{biz_hive_table}/"
commit="测试"
hive_table_temp="{hive_database}_temp.{biz_hive_table}_temp"
hive_hdfs_path_temp="/hive_data/ods_data/{hive_database}_temp/{biz_hive_table}_temp/"
commit_temp="测试"
sqoop_connect="jdbc:mysql://{host}:{port}/{target_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false"
sqoop_username="{username}"
sqoop_password="{password}"
split_column="{split_column}"
env="{env}"
num_mappers="{num_mappers}"
#需要修改结束

"""


OVERWRITE_FORMAT_ORIGIN = r"""

# 模式
mode=$1
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
  thedate=$2
else
  thedate=`date -d "-1 days "  "+%Y-%m-%d"`
fi
thedate_end=`date -d "${thedate}  1 days "  "+%Y-%m-%d"`
#由于中间缓存表不需要保留数据，每次覆盖写固定分区
thedate_temp="0000-00-00"

sqoop_target_dir="/hive_data/origin_data/${hive_table}/${thedate}"

if [ "${env}" = "online" ] || [ "${env}" = "test" ];then
  hive='/usr/hdp/3.0.0.0-1634/hive/bin/hive -n hdfs -p fire_hunliji -e '
elif [ "${env}" = "offline" ];then
  hive='/usr/hdp/2.6.3.0-235/hive/bin/hive -e '
else
  echo ""
fi

function del_table(){
  echo "============================================================================"
  echo "${hive_table} del_table start"
  sql="drop table if exists ${hive_table};"
  ${hive} "${sql}"
  hdfs dfs -rm -r ${hive_hdfs_path}
  echo "${hive_table} del_table end"
}

function create_table(){
  echo "============================================================================"
  echo "${hive_table} create_table start"
  sql="create external table ${hive_table}(
    ${create_hivetable_columns}
  )
  COMMENT '${commit_temp}'
  PARTITIONED BY (\`thedate\` string)
  row format delimited
  fields terminated by '\001'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  location '${hive_hdfs_path}'
  tblproperties (\"orc.compress\"=\"SNAPPY\");"
  ${hive} "${sql}"
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} create_table end"
}

function sqoop_data(){
  echo "============================================================================"
  echo "${hive_table} sqoop_data start"
  echo "${hive_table} sqoop_data overwrite"
  sql="
  select
  ${columns}
  from ${mysql_table_name}
  where 1=1  "
  sqoop import \
  --connect ${sqoop_connect} \
  --username ${sqoop_username} \
  --password ${sqoop_password} \
  --target-dir ${sqoop_target_dir} \
  --delete-target-dir \
  --hive-drop-import-delims \
  --query "${sql} and \$CONDITIONS" \
  --num-mappers ${num_mappers} \
  --split-by ${split_column} \
  --fields-terminated-by '\001' \
  --lines-terminated-by '\n' \
  --compress \
  --compression-codec SNAPPY \
  --null-string '\\N' \
  --null-non-string '\\N'

  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} sqoop_data end"
}

function load_data(){
  echo "============================================================================"
  echo "${hive_table} load_data start"
  sql="
  set hive.exec.dynamic.partition.mode=nonstrict;
  load data inpath '${sqoop_target_dir}' OVERWRITE into table
  ${hive_table} partition(thedate='${thedate_temp}');"
  ${hive} "${sql}"
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} load_data end"
}






case ${mode} in
	"first")
	del_table
	create_table
	sqoop_data 
	load_data
;;
	"overwrite")
	sqoop_data
	load_data
;;
	"del_table")
	del_table
;;
	"create_table")
	create_table
;;
	"sqoop_data")
	sqoop_data
;;
	"load_data")
	load_data
;;
*)
  echo "Usage: $0 {overwrite|first}"
esac
"""
