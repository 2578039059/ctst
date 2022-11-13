#!/usr/bin/python
# -*- coding: UTF-8 -*-
# import sys
#
# reload(sys)
# sys.setdefaultencoding("UTF-8")
# sys.path.append("./promo_analytics_test")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import promo_analytics.examples.data.preprocess as preprocess

spark = SparkSession.builder.appName("baseline_time_series").getOrCreate()

# 读取输入表
df_input = spark.sql("""select a.*, b.c_code from lhsmartonline.backup_baseline_est_upgrade_step5_0 a
LEFT JOIN lhsmartprom.olap_item_info b
ON a.product_key = b.sku_id
""")
# df_input = spark.sql("""select a.*, b.c_code from lhsmartprom.model_baseline_est_upgrade_step5_0 a
# LEFT JOIN lhsmartprom.olap_item_info b
# ON a.product_key = b.sku_id
# """)

df_input.show()

# 调用基线工具包函数来准备数据
df_input = preprocess.prepare(df_input, spark)

# 选择基线数据预处理颗粒度
category = "all"

mass_daily_sales_input = preprocess.exe(df = df_input, category = category)

mass_daily_sales_data = mass_daily_sales_input.withColumn(
    "date_key",
    F.from_unixtime(F.unix_timestamp(F.col("date_key"), "yyyy-MM-dd"), "yyyyMMdd"),
)

# 确保没有 date_key 为空的行
mass_daily_sales_data = mass_daily_sales_data.filter(F.col("date_key").isNotNull())

mass_daily_sales_data = mass_daily_sales_data.select(
    [
        "format_name",
        "product_key",
        "item_main_category_code",
        "item_main_category",
        "item_category_code",
        "item_category",
        "item_sub_category",
        "item_segment",
        "date_key",
        "store_key",
        "quantity_total",
        "amount_price",
        "amount_discount",
        "quantity_discount",
        "promotion_key",
        "amount_price_ex_vat",
        "unit_cost_sum",
        "promo_flag",
        # added
        "f_holiday",
        "no_days_since_promo",
        "no_stores_sell_product",
        "footfall_count",
        "footfall_idx",
        "footfall_count_cat",
        "footfall_idx_cat",
        "ALPHA_PRC_EVENT_CD",
        "mth_idx"
    ]
)

# 保存输出表
spark.sql("DROP TABLE IF EXISTS lhsmartonline.back_baseline_est_upgrade_step6_2_all")
mass_daily_sales_data.write.saveAsTable("lhsmartonline.back_baseline_est_upgrade_step6_2_all")
