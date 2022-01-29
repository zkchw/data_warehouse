#!/usr/bin/env python
# -*- coding: utf-8 -*-

FORMAT = r"""#!/bin/bash
export HADOOP_USER_NAME=hdfs
#export HADOOP_CONF_DIR=/usr/hdp/3.0.0.0-1634/hadoop/conf

#需要修改开始
mysql_table_name="{source_table}"
columns="
   {columns}
"
hive_database="{hive_database}"
biz_hive_table="{biz_hive_table}"
hive_table="{hive_database}.{biz_hive_table}"
sqoop_connect="jdbc:mysql://{host}:{port}/{target_database}?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&autoReconnect=true&failOverReadOnly=false"
sqoop_username="{username}"
sqoop_password="{password}"
split_column="{split_column}"
env="{env}"
num_mappers="{num_mappers}"
#需要修改结束

"""


ORIGIN = r"""

# 模式
mode=$1
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
  thedate=$2
else
  thedate=`date -d "-1 days "  "+%Y-%m-%d"`
fi
thedate_end=`date -d "${thedate}  1 days "  "+%Y-%m-%d"`

if [ "${env}" = "online" ] || [ "${env}" = "test" ];then
  hive='/usr/hdp/3.0.0.0-1634/hive/bin/hive -n hdfs -p fire_hunliji -e '
elif [ "${env}" = "offline" ];then
  hive='/usr/hdp/2.6.3.0-235/hive/bin/hive -e '
else
  echo ""
fi

function sqoop_data(){

    if [ "$mode" = "overwrite" ];then
            ${hive} "drop table if exists ${hive_table}"
          sql="
          select
          ${columns}
          from ${mysql_table_name}
          where 1=1 "
          sqoop import \
          --connect ${sqoop_connect} \
          --username ${sqoop_username} \
          --password ${sqoop_password} \
          --num-mappers ${num_mappers} \
          --split-by ${split_column} \
          --query "${sql} and \$CONDITIONS" \
          --hive-drop-import-delims \
          --hcatalog-database ${hive_database} \
          --hcatalog-table ${biz_hive_table} \
          --create-hcatalog-table \
          --hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
          --hcatalog-partition-keys thedate \
          --hcatalog-partition-values ${thedate} \
          --class-name ${hive_database}.${biz_hive_table};
    elif [ "$mode" = "append-all" ];then
             ${hive} "alter table ${hive_table} drop if exists partition(thedate='${thedate}')"
              sql="
              select
              ${columns}
              from ${mysql_table_name}
              where 1=1 "
              sqoop import \
              --connect ${sqoop_connect} \
              --username ${sqoop_username} \
              --password ${sqoop_password} \
              --num-mappers ${num_mappers} \
              --split-by ${split_column} \
              --query "${sql} and \$CONDITIONS" \
              --hive-drop-import-delims \
              --hcatalog-database ${hive_database} \
              --hcatalog-table ${biz_hive_table} \
              --hcatalog-storage-stanza 'stored as orcfile tblproperties ("orc.compress"="SNAPPY")' \
              --hcatalog-partition-keys thedate \
              --hcatalog-partition-values ${thedate} \
              --class-name ${hive_database}.${biz_hive_table};
    else
        echo "${hive_table} mode wrong"
    fi

  if [ $? -eq 0 ]
  then
      echo 0
  else
      exit 1
  fi
}

sqoop_data
"""
