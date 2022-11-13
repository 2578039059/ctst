#!/usr/bin/python
# -*- coding: UTF-8 -*-
# import sys
#
# reload(sys)
# sys.setdefaultencoding("UTF-8")
# sys.path.append("./promo_analytics_test")

from pyspark.sql.types import IntegerType

from pyspark.sql import SparkSession
from sklearn.linear_model import ElasticNet
import numpy as np
from sklearn.metrics import r2_score
from pyspark.sql.types import *

from promo_analytics.mass.baseline_builder_reg import BaselineBuilderSpark

spark = SparkSession.builder.appName("baseline_elastic_net").getOrCreate()

mass_daily_sales_data = spark.sql(
    """
    select * from lhsmartonline.back_baseline_est_upgrade_step6_2_all
    """
    # model_baseline_est_upgrade_step6_2_all
)

# 将节假日标签转变为正确的类型
mass_daily_sales_data = mass_daily_sales_data.withColumn(
    "f_holiday", mass_daily_sales_data["f_holiday"].cast(IntegerType())
)



# elasticnet需要用到的回归方程
def baseline_regression_fit_cb(data):  ###for elastic net
    print('是否进入该函数')
    print(data)
    key, vecs = data
    format_name, item_main_category_code, item_segment, product_key, nb_promo_related_features = key
    date_keys = [vec[0] for vec in vecs]
    print("baseline_regression_fit_cb:1")
    y = [vec[1] for vec in vecs]
    X = [vec[2:] for vec in vecs]

    # Make baseline by setting promo related features to 0
    # 将促销相关的特征都设为 0
    X_fake = [[0 if i < nb_promo_related_features else e for i, e in enumerate(ax)] for ax in X]

    if (len(y) == 0) or (len(y) == 0):
        return []

    clf = ElasticNet(alpha = 0.1, l1_ratio=0.7, fit_intercept=False, positive=True)
    clf.fit(X, y)
    y_pred = clf.predict(X)
    # Save errors
    error = float(np.mean([abs(a-b)/a for a, b in zip(y,y_pred)
                           if (a != 0) and (b != 0) and (~np.isnan(a)) and (~np.isnan(b))]))
    if ((clf.coef_[0]<0) or (clf.coef_[1]<0) or (clf.coef_[2]<0) or (clf.coef_[3]<0)):
        r_squared = float(0)
    elif ((clf.coef_[0]==0) and (clf.coef_[1]==0) and (clf.coef_[2]==0) and (clf.coef_[3]==0)):
        r_squared = float(0)
    else:
        r_squared = float(r2_score(y, y_pred))

    # Now, make predictions for the whole year and save the data
    y_true_year = y
    y_pred_year = [float(e) for e in clf.predict(X)]
    y_base_year = [float(e) for e in clf.predict(X_fake)]
    N = len(y)
    return zip(N*[format_name], N*[item_main_category_code], N*[item_segment], N*[product_key], N*[r_squared], N*[error], date_keys,
               y_true_year, y_pred_year, y_base_year)

def run_baseline_builder(spark_session,
                         mass_daily_sales,
                         regression_func):

    baseline_builder = BaselineBuilderSpark(spark_session, regression_func)
    print('还没运行run')
    predicted_baseline = baseline_builder.run(mass_daily_sales)
    print('运行run了')
    return predicted_baseline

# 调用基线工具包函数来预测基线
predicted_baseline_sales_df = run_baseline_builder(
    spark_session=spark,
    mass_daily_sales=mass_daily_sales_data,
    regression_func=baseline_regression_fit_cb
)


# spark.sql("DROP TABLE IF EXISTS model_baseline_est_upgrade_step6_3_all_elastic_net")
#
# predicted_baseline_sales_df = predicted_baseline_sales_df.select(
#     [
#         "format_name",
#         "item_segment",
#         "item_main_category_code",
#         "product_key",
#         "date_key",
#         "reg_r_squared",
#         "reg_mean_error",
#         "reg_true_sales",
#         "reg_pred_sales",
#         "reg_pred_baseline",
#         "item_category_code",
#         "reg_number_stores_on_promo",
#         "reg_frac_stores_on_promo",
#         "promo_price",
#         "unpromo_price",
#     ]
# )
#
# predicted_baseline_sales_df.count()
# predicted_baseline_sales_df.show()
#
# predicted_baseline_sales_df.write.saveAsTable(
#     "model_baseline_est_upgrade_step6_3_all_elastic_net"
# )
