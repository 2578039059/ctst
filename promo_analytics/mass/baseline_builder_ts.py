#! /usr/bin/env python

from promo_analytics.mass.baseline_ts import BaselineTimeSeriesSpark
from pyspark.sql.types import *


class BaselineBuilderSpark(object):
    """Build the baseline using both time series and multivariate regression"""
    """使用时间序列和多元回归建立基线"""

    def __init__(self, spark_session):
        print("timeseries..start")
        self.time_series = BaselineTimeSeriesSpark(spark_session)

    def run(self, df):
        """Main execution method

        Three simple steps are needed:
            * Time series analysis for all SKU-format
            * Multivariate regression for all SKUs-format
            * Final reconciliation of both models, for each SKU-format the
            better model is selected.
        """
        """
        主要执行方法:
         需要三个简单的步骤：
             *所有SKU格式的时间序列分析
             *所有SKU格式的多元回归
             *两种型号的最终核对，为每种SKU选择了更适合的模型。
         """
        # added to drop new feature columns
        print("time series started")

        return self.time_series.run(df)
