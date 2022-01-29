#!/usr/bin/env python
# -*- coding: utf-8 -*-

UPDATE_FORMAT = r"""#!/bin/bash
export HADOOP_USER_NAME=hdfs
#export HADOOP_CONF_DIR=/usr/hdp/3.0.0.0-1634/hadoop/conf

#需要修改开始
mysql_table_name="{source_table}"
create_column="{create_column}"
update_column="{update_column}"
update_need_end="{update_need_end}"

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
sqoop_connect="jdbc:mysql://{host}:{port}/{source_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false"
sqoop_username="{username}"
sqoop_password="{password}"
split_column="{split_column}"
env="{env}"
num_mappers="{num_mappers}"
#需要修改结束

"""


UPDATE_FORMAT_ORIGIN = r"""

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
if [ "${update_need_end}" = "N" ];then
    thedate_end="3000-00-00"
elif [ "${update_need_end}" = "Y" ];then
    thedate_end=`date -d "${thedate}  1 days "  "+%Y-%m-%d"`
else
  echo "update_need_end配置出错  Y或者N"
fi

sqoop_target_dir="/hive_data/origin_data/${hive_table}/${thedate}"

if [ "${env}" = "online" ] || [ "${env}" = "test" ];then
  hive='/usr/hdp/3.0.0.0-1634/hive/bin/hive -n hdfs -p fire_hunliji -e '
elif [ "${env}" = "offline" ];then
  hive='/usr/hdp/2.6.3.0-235/hive/bin/hive -e '
else
  echo ""
fi

if [ "${env}" = "online" ] || [ "${env}" = "test" ];then
  sparksql='/usr/hdp/3.0.0.0-1634/spark2/bin/spark-sql '
elif [ "${env}" = "offline" ];then
  sparksql='/usr/hdp/2.6.3.0-235/spark2/bin/spark-sql '
else
  echo ""
fi

SPARK_SUBMIT_INFO_UPDATE=${sparksql}"
--master yarn
--deploy-mode client
--driver-memory 1g
--executor-memory 4g
--num-executors 5
--executor-cores 2
--name ${hive_table}
--conf spark.sql.parquet.writeLegacyFormat=true
--conf spark.sql.sources.partitionOverwriteMode=dynamic
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.minNumPostShufflePartitions=1
--conf spark.sql.adaptive.maxNumPostShufflePartitions=200
--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728
--conf spark.sql.adaptive.shuffle.targetPostShuffleRowCount=1000000
--hiveconf hive.exec.max.dynamic.partitions=10000
--hiveconf hive.exec.dynamic.partition.mode=nonstrict
"
SPARK_SUBMIT_INFO_OVERWRITE=${sparksql}"
--master yarn
--deploy-mode client
--driver-memory 1g
--executor-memory 4g
--num-executors 10
--executor-cores 2
--name ${hive_table}
--conf spark.sql.parquet.writeLegacyFormat=true
--conf spark.sql.sources.partitionOverwriteMode=dynamic
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.minNumPostShufflePartitions=30
--conf spark.sql.adaptive.maxNumPostShufflePartitions=300
--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728
--conf spark.sql.adaptive.shuffle.targetPostShuffleRowCount=1000000
--hiveconf hive.exec.max.dynamic.partitions=10000
--hiveconf hive.exec.dynamic.partition.mode=nonstrict
"

function create_table_temp(){
  echo "============================================================================"
  echo "${hive_table} create_table_temp start"
  sql="create external table ${hive_table_temp}(
    ${create_hivetable_columns}
  )
  COMMENT '${commit_temp}'
  PARTITIONED BY (\`thedate\` string)
  row format delimited
  fields terminated by '\001'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  location '${hive_hdfs_path_temp}'
  tblproperties (\"orc.compress\"=\"SNAPPY\");"
  ${hive} "${sql}"
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} create_table_temp end"
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
  if [ "$1" = "overwrite" ];then
    echo "${hive_table} sqoop_data overwrite"
    #第一次全量抽数据sql
    sql="
    select
    ${columns}
    from ${mysql_table_name}
    where ${create_column} >'2000-01-01' and ${create_column} < '${thedate_end}' "
    
    sqoop import \
    --connect ${sqoop_connect} \
    --username ${sqoop_username} \
    --password ${sqoop_password} \
    --target-dir ${sqoop_target_dir} \
    --delete-target-dir \
    --hive-drop-import-delims \
    --query "${sql} and \$CONDITIONS" \
    --num-mappers 20 \
    --split-by ${split_column} \
    --fields-terminated-by '\001' \
    --lines-terminated-by '\n' \
    --compress \
    --compression-codec SNAPPY \
    --null-string '\\N' \
    --null-non-string '\\N'

  elif [ "$1" = "update" ];then
    echo "${hive_table} sqoop_data update"
    #正常抽数据sql
    sql="
    select
    ${columns}
    from ${mysql_table_name}
    where
    ((${create_column} >= '${thedate}' and ${create_column} < '${thedate_end}')
    or
    (${update_column} >= '${thedate}' and ${update_column} < '${thedate_end}')) and ${create_column} >'2000-01-01' "
    
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
  else
    echo "${hive_table} sqoop mode wrong"
  fi
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} sqoop_data end"
}




function del_table_temp(){
  echo "============================================================================"
  echo "${hive_table_temp} del_table_temp start"
  sql="drop table if exists ${hive_table_temp};"
  ${hive} "${sql}"
  hdfs dfs -rm -r ${hive_hdfs_path_temp}
  echo "${hive_table_temp} del_table_temp end"
}

function load_data(){
  echo "============================================================================"
  echo "${hive_table} load_data start"
  sql="
  set hive.exec.dynamic.partition.mode=nonstrict;
  load data inpath '${sqoop_target_dir}' OVERWRITE into table
  ${hive_table_temp} partition(thedate='${thedate_temp}');"
  ${hive} "${sql}"
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} load_data end"
}



function del_table(){
  echo "============================================================================"
  echo "${hive_table} del_table start"
  sql="drop table if exists ${hive_table};"
  ${hive} "${sql}"
  hdfs dfs -rm -r ${hive_hdfs_path}
  echo "${hive_table} del_table end"
}

function update_data(){
  echo "============================================================================"
  echo "${hive_table} update_data start"

  if [ "$1" = "overwrite" ];then
    sql="
    insert overwrite table ${hive_table} partition(thedate)
    select
    ${columns},
    thedate
    from
    (
      select
      ${columns},
      date_format(created_at,'yyyy-MM-dd') as thedate
      from ${hive_table_temp}
      where thedate='${thedate_temp}'
    )
    distribute by thedate, cast(rand() * 1 as int);"
    echo ${sql}
    ${SPARK_SUBMIT_INFO_OVERWRITE} -e "${sql}"
  elif [ "$1" = "update" ];then
      array=(${columns//,/ })
      sql="
      insert overwrite table ${hive_table} partition(thedate)
      select "

      for var in ${array[@]}
      do
         sql=${sql}"if(new.id is null,old.${var},new.${var}),"
      done
      sql=${sql}"
          date_format(if(new.id is null,old.created_at,new.created_at),'yyyy-MM-dd') as thedate
          from (
            select
            *
            from ${hive_table} where thedate in
            (
              select
              thedate
              from
              (
                select
                date_format(created_at,'yyyy-MM-dd') as thedate
                from ${hive_table_temp}
                where
                thedate='${thedate_temp}'
              )
              group by thedate
            )
          )old
          full outer join
          (
            select
            *
            from ${hive_table_temp} where thedate='${thedate_temp}'
          )new
          on old.id=new.id
          distribute by thedate, cast(rand() * 1 as int);"
      echo ${sql}
      ${SPARK_SUBMIT_INFO_UPDATE} -e "${sql}"
  else
    echo "${hive_table} update_data mode wrong"
  fi
  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
  echo "${hive_table} update_data end"
}
source activate base
case ${mode} in
	"overwrite")
	del_table_temp
	del_table
	create_table_temp
	create_table
	sqoop_data "overwrite"
	load_data
	update_data "overwrite"
;;
	"update")
	sqoop_data "update"
	load_data
	update_data "update"
;;
	"del_table_temp")
	del_table_temp
;;
	"del_table")
	del_table
;;
	"create_table_temp")
	create_table_temp
;;
	"create_table")
	create_table
;;
	"sqoop_data_overwrite")
	sqoop_data "overwrite"
;;
	"sqoop_data_update")
	sqoop_data "update"
;;
	"load_data")
	load_data
;;
	"update_data_overwrite")
	update_data "overwrite"
;;
	"update_data_update")
	update_data "update"
;;
*)
  echo "Usage: $0 {overwrite|update}"
  exit 1
esac

source deactivate
"""
