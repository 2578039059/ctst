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

# df_input.show()
df_input.write.insertInto('tmp_dsvd_xs')