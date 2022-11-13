#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import imp
imp.reload(sys)
# reload(sys)#报错注释一下罢了
# sys.setdefaultencoding("UTF-8")
sys.path.append("./promo_analytics_test")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
from promo_analytics.mass.baseline_builder_ts import BaselineBuilderSpark


spark = SparkSession.builder.appName("baseline_time_series").getOrCreate()

# 读取输入表
# mass_daily_sales_data的字面意思是 “大量的日常销售”
mass_daily_sales_data = spark.sql(
    """
    SELECT *
    FROM lhsmartprom.tmp_xjd_baseline_est_step6_2_all a
    """
#    正式环境  测试使用tmp表
#     model_baseline_est_upgrade_step6_2_all
)


def run_baseline_builder(spark_session, mass_daily_sales):

    baseline_builder = BaselineBuilderSpark(spark_session)
    predicted_baseline = baseline_builder.run(mass_daily_sales)
    return predicted_baseline

# 调用基线工具包函数来预测基线
predicted_baseline_sales_df = run_baseline_builder(
    spark_session=spark,
    mass_daily_sales=mass_daily_sales_data
)

# predicted_baseline_sales_df.show()
print(dir(predicted_baseline_sales_df))
# print(predicted_baseline_sales_df.count())
print(predicted_baseline_sales_df.__class__)
print(predicted_baseline_sales_df.schema)

predicted_baseline_sales_df = (
    predicted_baseline_sales_df.withColumn("Date", F.col("Date").cast(StringType()))
        .withColumn("date_time", F.col("date_time").cast(StringType()))
        .withColumn("yr", F.col("yr").cast(IntegerType()))
        .withColumn("wk", F.col("wk").cast(IntegerType()))
        .withColumn("promo_flag_0", F.col("promo_flag_0").cast(IntegerType()))
        .withColumn("num_stores_0", F.col("num_stores_0").cast(IntegerType()))
        .withColumn("promo_flag_1", F.col("promo_flag_1").cast(IntegerType()))
        .withColumn("num_stores_1", F.col("num_stores_1").cast(IntegerType()))
)
print(predicted_baseline_sales_df.schema)

# 保存输出表
spark.sql("DROP TABLE IF EXISTS lhsmartprom.tmp_xjd_baseline_est_step6_3_all")

# model_baseline_est_upgrade_step6_3_all

predicted_baseline_sales_df.select(
    [
        "format_name",
        "item_main_category_code",
        "item_segment",
        "product_key",
        "wd",
        "yr",
        "wk",
        "date_key",
        "date",
        "quantity_total_0",
        "amount_discount_0",
        "quantity_discount_0",
        "amount_price_0",
        "promo_flag_0",
        "num_stores_0",
        "quantity_total_1",
        "amount_discount_1",
        "quantity_discount_1",
        "amount_price_1",
        "promo_flag_1",
        "num_stores_1",
        "total_sales_quantity",
        "gusbl_rev_bline",
        "gusbl_qty_bline",
        "unprm_qty",
        "unprm_rev",
        "prev_index",
        "post_index",
        "day_idx",
        "wk_idx",
        "time_pred_baseline",
        "date_time",
        "time_r_squared"
    ]
).write.saveAsTable("lhsmartprom.tmp_xjd_baseline_est_step6_3_all")
