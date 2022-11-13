#!/usr/bin/python
# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
# import sys
import imp
# imp.reload(sys)
# reload(sys)#报错注释一下罢了
# sys.setdefaultencoding("UTF-8")
# sys.path.append("./promo_analytics_test")

## 此流程生成了model_baseline_est_upgrade_step4_1_0表来不全正在进行的促销的信息
ptTableName = "mc_test_pt_table"
spark = SparkSession.builder.appName("test").getOrCreate()
ld=spark.sql("SELECT * FROM lhsmartonline.back_baseline_est_upgrade_step6_2_all limit 100")
ld.show()
# ld.createOrReplaceTempView("%s_tmp_view" % ptTableName)
# ld.write.insertInto(ptTableName) # 动态分区 insertInto语义
# ld.write.csv('C:\\Users\Administrator\Documents\cx_test\cdcfdsa.csv')
ld.createTempView('tmo_ds')
# .csv('C:\临时文件\cdcfdsa.csv')
# df=spark.sql("select * from lhsmartprom.model_baseline_est_upgrade_step4  limit 10")
# print(df.count())