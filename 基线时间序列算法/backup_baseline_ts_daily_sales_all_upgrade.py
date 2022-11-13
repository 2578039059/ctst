import sys

# reload(sys)
# sys.setdefaultencoding("UTF-8")
# sys.path.append("./promo_analytics_test")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import promo_analytics.examples.data.preprocess as preprocess

spark = SparkSession.builder.appName("baseline_time_series").getOrCreate()


# df_input = spark.sql("""select a.*, b.c_code from lhsmartprom.model_baseline_est_upgrade_step5_0 a
# LEFT JOIN lhsmartprom.olap_item_info b
# ON a.product_key = b.sku_id
# """)

df_input = spark.sql(""" select  * from  lhsmartonline.backup_baseline_est_upgrade_step5 """)
df_input.show()
# df_input = preprocess.prepare(df_input, spark)
category = "all"

mass_daily_sales_input = preprocess.exe(df = df_input, category = category)

mass_daily_sales_data = mass_daily_sales_input.withColumn(
    "date_key",
    F.from_unixtime(F.unix_timestamp(F.col("date_key"), "yyyy-MM-dd"), "yyyyMMdd"),
)

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
#
spark.sql("TRUNCATE TABLE lhsmartonline.back_baseline_est_upgrade_step6_2_all")
mass_daily_sales_data.write.insertInto('lhsmartonline.back_baseline_est_upgrade_step6_2_all')
# spark.sql("DROP TABLE IF EXISTS lhsmartprom.model_baseline_est_upgrade_step6_2_all")
# mass_daily_sales_data.write.saveAsTable("lhsmartprom.model_baseline_est_upgrade_step6_2_all")
