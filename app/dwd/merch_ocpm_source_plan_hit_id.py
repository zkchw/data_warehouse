# -*- coding: utf-8 -*-
# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf
# from pyspark.sql.functions import lit
import datetime
import logging
import sys

# 格式化日志输出格式
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(filename)s][line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

"""
@ Description:
@ Author: sitan
conf_begin
@state:        on                
conf_end
resource_begin
@driver_memory:              
@num_executors:              
@executor_memory:            
@executor_cores:   
@deploy_mode:       client            
resource_end
"""


# 日期格式化
YYYYmmdd = '%Y-%m-%d'

# 库名
DATABASE_NAME='dwd'

# 表名 **需要和文件名一样
TABLE_NAME='merch_ocpm_source_plan_hit_id'

# 表名描述
TABLE_DESC='xxxx平台页面点击数据'

# get spark session
def get_ss():
    pass
    # return SparkSession.builder.config(conf=SparkConf()).enableHiveSupport().getOrCreate()

# 创建表
def t_ddl(spark,database,table,table_desc):
    sql = """
        create table if not exists %s.%s
        (
            merchant_id                 string  comment '商家id',
            merchant_name               string  comment '商家名称',
            product_type                int  comment '产品类型（1线索通 2智优投 3智推宝)',
            plan_id                     string  comment '计划ID',
            plan_name                   string  comment '计划名称',
            consume                     decimal(20,2)  comment '广告消耗金额',
            consume_log                 decimal(20,2)  comment '广告消耗金额,来源于日志',
            exposure_count              int  comment '展示次数',
            click_count                 int  comment '点击次数',
            click_user_count            int  comment '点击人数',
            convert_user_count          int  comment '转化人数',
            thousand_budget             decimal(18,2) comment '千次曝光扣费价格',
            expo_count             decimal(18,2) comment '曝光次数'
        ) comment '%s'
        partitioned by (thedate string comment '按天分区')
        stored as orc
    """%(database,table,table_desc)
    logging.info('DDL: '+sql)
    # spark.sql(sql)

# load date...
def load_date(spark, thedate, today, themonth, month_first, month_end):
    # 解决精度丢失问题
    # spark.sql("""set spark.sql.decimalOperations.allowPrecisionLoss=false""")
    sql_str = """
       with df_base as (
                    select 
                          merchant_id,
                          '' as merchant_name,
                          case 
                            when plan_kind = 'ocpm' then 1
                            when plan_kind = 'cpm_v2' then 2
                            when plan_kind = 'ztb_v2' then 3
                          end as product_type,
                          plan_id,
                          sum(case when is_pay_success='true' and event_type = 'element_view' and plan_kind = 'cpm_v2' then thousand_budget_1 else 0 end) as thousand_budget,
                          sum(case when is_pay_success='true' and event_type = 'element_view' and plan_kind = 'cpm_v2' then value_ratio else 0 end) as expo_count,
                          '' as plan_name,
                          cast(sum(case when is_pay_success='true' then thousand_budget_1*1.00/1000 else 0 end) as decimal(20,2)) as consume,
                          -- sum(thousand_budget_1*1.00/1000) as consume,
                          cast(sum(case when is_pay_success='true' and event_type = 'element_view' then value_ratio else 0 end) as int)  as  exposure_count,
                          sum(case when event_type = 'element_hit' then 1 else 0 end)  as  click_count,
                          count(distinct case when event_type = 'element_hit' and user_id > 0 then user_id 
                                              when event_type = 'element_hit' and user_id = -1 then visitor_id end) as click_user_count,
                         -- count(distinct case when event_type = 'element_convert' then user_id end) as convert_user_count,
                          '{thedate}' as thedate
                    from ods.merch_ocpm_source_log_id
                    where thedate='{thedate}'
                    group by merchant_id,case 
                            when plan_kind = 'ocpm' then 1
                            when plan_kind = 'cpm_v2' then 2
                            when plan_kind = 'ztb_v2' then 3
                          end,plan_id
             ),
            df_connect as (
             select 
                    connect_count,
                    plan_id,
                    merchant_id,
                    case 
                        when plan_kind = 'ocpm' then 1
                        when plan_kind = 'cpm_v2' then 2
                        when plan_kind = 'ztb_v2' then 3
                    end as plan_kind
              from ods.merch_ocpm_source_log_id 
              where plan_id is not null 
              and thedate = '{thedate}'
              and plan_id <> '' 
              and merchant_id is not null 
              -- and plan_kind='ocpm'
              ),
            df_consume as (
             select
                merchant_id,
                plan_id,
                bs_type,
                final_consume
             from 
             ods.merch_ocpm_source_log_id
             where thedate = '{thedate}'
             and merchant_id is not null 
             and merchant_id <> ''
             and plan_id is not null 
             and plan_id  <> ''
            )  
        select
            t.merchant_id,
            t.merchant_name,
            t.product_type,
            t.plan_id,
            t.plan_name,
            if(t2.final_consume is not null,t2.final_consume,0) as consume,
            t.consume as consume_log,
            t.exposure_count,
            t.click_count,
            t.click_user_count,
            if(t1.connect_count is not null,t1.connect_count,0) as convert_user_count,
            t.thousand_budget,
            t.expo_count,
            t.thedate
         from df_base t
         left join
         df_connect t1
         on t.merchant_id = cast(t1.merchant_id as varchar(50))
         and t.plan_id = t1.plan_id and t.product_type = t1.plan_kind
         left join 
         df_consume t2
         on t.merchant_id = cast(t2.merchant_id as varchar(50)) and t.product_type = t2.bs_type
         and t.plan_id = t2.plan_id
    """.format(thedate=thedate, today=today, themonth=themonth, month_first=month_first, month_end=month_end)
    logging.info('DML: ' + sql_str)
    # return spark.sql(sql_str).repartition(10)

#        cast(if(t.exposure_count is not null and t.exposure_count !=0 , t.click_count*1.00/t.exposure_count, 0) as decimal(20,4)) as click_rate,
#          cast(if(t.click_count is not null and t.click_count != 0 , t.convert_count*1.00/t.click_count, 0) as decimal(20,4)) as convert_rate,
# write to hive
def save_to_hive(df,database,table):
    logging.info('success!!!')
    # df.createOrReplaceTempView('table_tmp')
    # spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
    # spark.sql('set hive.exec.dynamic.partition=true')
    # spark.sql('set spark.sql.parquet.writeLegacyFormat=true')
    # spark.sql('set spark.sql.sources.partitionOverwriteMode=dynamic')
    #
    #
    # sql = """
    #     insert overwrite table %s.%s partition (thedate)
    #     select * from table_tmp
    # """%(database,table)
    # spark.sql(sql)


# 获得当月第一天
def get_month_first(thedate):
    return get_n_month_first(thedate, 0)


# 获取本周周一日期
def get_week_first(thedate):
    from datetime import timedelta
    today = datetime.datetime.strptime(str(thedate), "%Y-%m-%d")
    return datetime.datetime.strftime(today - timedelta(today.weekday()), "%Y-%m-%d")


# 获得当月最后一天
def get_month_end(thedate):
    day = datetime.datetime.strptime(thedate, "%Y-%m-%d")
    month = day.month
    year = day.year
    if month == 12:
        month = 0
        year = year + 1
    future_mouth_first = datetime.datetime(year, month + 1, 1, 23, 59, 59)
    this_month = future_mouth_first - datetime.timedelta(days=1)
    return this_month.strftime('%Y-%m-%d')


def get_n_month_first(thedate, num):
    day = datetime.datetime.strptime(thedate, YYYYmmdd)
    year = day.year
    month = day.month
    if day.month - num <= 0:
        year -= 1
        month = 12 + day.month - num
    else:
        month -= num
    future_mouth_first = datetime.datetime(year, month, 1, 23, 59, 59)
    return future_mouth_first.strftime(YYYYmmdd)


# 获取前一周周一和对应结束时间的日期
def get_before_1_week(thedate):
    day = datetime.datetime.strptime(thedate, YYYYmmdd)
    week_end_before = datetime.datetime.strftime(day - datetime.timedelta(7), YYYYmmdd)
    week_start_before = datetime.datetime.strftime(
        datetime.datetime.strptime(week_end_before, YYYYmmdd) - datetime.timedelta(day.weekday()), YYYYmmdd)
    return week_start_before


# 判断是是否是1，2，3号
def is_start(thedate):
    # 当前时间
    day = datetime.datetime.strptime(thedate, "%Y-%m-%d")
    today = day.day
    if today <= 3:
        return True
    return False


if __name__ == '__main__':
    # 初始化时间
    today = datetime.datetime.strftime(datetime.datetime.now(), YYYYmmdd)  # 今天
    thedate = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=1), YYYYmmdd)  # 昨天
    params = sys.argv
    if len(params) > 1:
        thedate = params[1]
        today = params[2]
    logging.info("传入参数 thedate: %s today: %s", str(thedate), str(today))
    themonth = get_month_first(thedate)  # 当前月第一天
    month_first = get_month_first(thedate)  # 当前月第一天
    month_end = get_month_end(thedate)  # 当前月最后一天


    # create spark session
    spark = get_ss()
    # 创建表
    # 创建表
    t_ddl(spark, DATABASE_NAME, TABLE_NAME, TABLE_DESC)
    # load date
    df = load_date(spark, thedate, today, themonth, month_first, month_end)
    # write to hive
    save_to_hive(df, DATABASE_NAME, TABLE_NAME)
    # 结束
    # spark.stop()
