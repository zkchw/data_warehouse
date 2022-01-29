#!/usr/bin/env python
# -*- coding: utf-8 -*-

# azkaban同步任务脚本
# 第一步：执行同步脚本
# 第二步：记录同步脚本执行结果写入mysql
BIZ_TASK = r"""#!/bin/bash

python {result_check} {az_env} {job_name}
task_status=$?
echo "task_status: " $task_status
if [[ "$task_status" == 0 ]];then
    echo "任务今日已执行成功过，退出执行"
    exit 0
fi

python {result_save} {az_env} {job_name} running
sh {biz_task_name} {mode}
extract_result=$?
echo "extract_result: " $extract_result
if [[ "$extract_result" == 0 ]];then
    echo "同步任务执行成功"
    python {result_save} {az_env} {job_name} success
else
    echo "同步任务执行失败"
    python {result_save} {az_env} {job_name} failed
fi
exit $extract_result
"""

# python 脚本任务
# 第一步：检查依赖同步任务完成情况
# 第二部：同步任务完成则继续，否则抛出失败
PYTHON_TASK = r"""#!/bin/bash


python {result_check} {az_env} {job_name}
task_status=$?
echo "task_status: " $task_status
if [[ "$task_status" == 0 ]];then
    echo "任务今日已执行成功过，退出执行"
    exit 0
fi

python {check_etl_ready} {az_env} {job_name}
us_streaming_status=$?
echo "us_streaming_status: " $us_streaming_status
if [[ "$us_streaming_status" == 0 ]];then
    echo "上游任务执行成功"
    python {result_save} {az_env} {job_name} running
    {command}
    python_result=$?
    echo "python_result: " $python_result
    if [[ "$python_result" == 0 ]];then
        echo "任务执行成功"
        python {result_save} {az_env} {job_name} success
    else
        echo "任务执行失败"
        python {result_save} {az_env} {job_name} failed
    fi
    exit $python_result
else
    python {result_save} {az_env} {job_name} failed
    exit 1
fi
"""

ETL_TASK = r"""#!/bin/bash


python {result_check} {az_env} {job_name}
task_status=$?
echo "task_status: " $task_status
if [[ "$task_status" == 0 ]];then
    echo "任务今日已执行成功过，退出执行"
    exit 0
fi

python {check_etl_ready} {az_env} {job_name}
us_streaming_status=$?
echo "us_streaming_status: " $us_streaming_status
if [[ "$us_streaming_status" == 0 ]];then
    echo "上游任务执行成功"
    python {result_save} {az_env} {job_name} running
    {command}
    spark_result=$?
    echo "spark_result: " $spark_result

    if [[ "$spark_result" == 0 ]];then
        echo "任务执行成功"
        python {result_save} {az_env} {job_name} success
    else
        echo "任务执行失败"
        python {result_save} {az_env} {job_name} failed
    fi
    exit $spark_result
else
    python {result_save} {az_env} {job_name} failed
    exit 1
fi
"""
