#! /usr/bin/env python

from promo_analytics.mass.baseline_reg import BaselineRegressionSpark
from pyspark.sql.types import *


class BaselineBuilderSpark(object):
    """Build the baseline using both time series and multivariate regression"""

    def __init__(self, spark_session, regression_func=None):
        print("regression..start")
        self.regression = BaselineRegressionSpark(spark_session, regression_func)

    def run(self, df):
        print("BaselineBuilderSpark 的run函数中")
        """Main execution method

        Three simple steps are needed:
            * Time series analysis for all SKU-format
            * Multivariate regression for all SKUs-format
            * Final reconciliation of both models, for each SKU-format the
            better model is selected.
        """
        # print("The dataset input for regression :{}".format(df.head(10)))

        return self.regression.run(df)
