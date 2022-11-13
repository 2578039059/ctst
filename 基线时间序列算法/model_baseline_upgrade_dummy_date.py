#!/usr/bin/python
# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

## 此流程生成了model_baseline_est_upgrade_step4_1_0表来不全正在进行的促销的信息

spark = SparkSession.builder.appName("baseline_time_series").getOrCreate()

df = spark.sql("select * from tmp_model_baseline_est_upgrade_step4  ")
df.orderBy('product_key','store_key','date_key').show()





## 模型开始时间
tool_end_date = spark.sql(
    """
    select value from olap_parameters where parameter = 'tool_end_date'
    """
).rdd.flatMap(lambda x: x).collect()[0][0]

## 模型结束时间
tool_start_date = spark.sql(
    """
    select value from olap_parameters where parameter = 'tool_start_date'
    """
).rdd.flatMap(lambda x: x).collect()[0][0]



df_final_sub_4 = df.withColumn("start_date", F.lit(tool_start_date).cast('date')).withColumn("end_date", F.lit(tool_end_date).cast('date'))
df_final_sub_4.orderBy('product_key','store_key','date_key').show()





### filter by date_key between [end_date-365, end_date]
### 用[en_date-365, end_date]来过滤date_key字段
df_post = df_final_sub_4.filter((F.col('date_key')>F.date_sub(F.col('end_date'),365))
                                & (F.col('date_key')<=F.col('end_date')))

df_post.orderBy('product_key','store_key','date_key').show()

### date_key + 1 年
df_post = df_post.withColumn('date_key', F.date_add(F.col('date_key'),365))
window = Window.partitionBy('format_name', 'item_segment', 'item_main_category_code', 'product_key')
df_post = df_post.withColumn('min_date_key', F.min('date_key').over(window))
df_post.orderBy('product_key','store_key','date_key').show()

### item level filter by date between [min_date, min_date + 60]
### 用[min_date, min_date + 60]来过滤date_key
df_post = df_post.filter(F.col('date_key') < F.date_add(F.col('min_date_key'), 60)) \
    .drop('min_date_key')
df_post.orderBy('product_key','store_key','date_key').show()

df_final_sub_4 = df_final_sub_4.drop('start_date', 'end_date')
df_final_sub_4.orderBy('product_key','store_key','date_key').show()

# spark.sql("drop table if exists tmp_model_baseline_est_upgrade_step4_1_0")
# df_post.select(df_final_sub_4.columns).write.saveAsTable("tmp_model_baseline_est_upgrade_step4_1_0")