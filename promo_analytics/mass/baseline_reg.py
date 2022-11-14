#! /usr/bin/env python


from pyspark.sql.types import *
from pyspark.sql import functions as F
from .baseline_reg_base import BaselineRegressionBase


class BaselineRegressionSpark(BaselineRegressionBase):
    """Baseline regression in sparkself.

    The :meth:`run` method takes as argument the aggretation data for mass
    promos and return the baseline dataframe
    """

    def __init__(self, spark_session, regression_func):
        BaselineRegressionBase.__init__(self)
        self.spark_session = spark_session
        self.regression_func = regression_func

    def fit(self, df):
        print('执行第三步fit')
        reg_features = self.reg_features
        print(reg_features)
        print(self.regression_func)

        nb_promo_related_features = self.nb_promo_related_features
        print(self.regression_func)

        regress_rdd = (
            df
            ####.na.fill(0)
            .fillna(0)
            .rdd.map(
                lambda row: (
                    (
                        row.format_name,
                        row.item_main_category_code,
                        row.item_segment,
                        row.product_key,
                        nb_promo_related_features,
                    ),
                    [row.date_key, row.quantity_total] + [row[c] for c in reg_features],
                )
            )
            .groupByKey()
            .flatMap(self.regression_func)
        )

        # print(regress_rdd.head())
        # regress_rdd.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\mregress_rdd1.csv')


        regress_df = self.spark_session.createDataFrame(
            regress_rdd,
            StructType(
                [
                    StructField("format_name", StringType(), True),
                    StructField("item_main_category_code", StringType(), True),
                    StructField("item_segment", StringType(), True),
                    StructField("product_key", StringType(), True),
                    StructField("reg_r_squared", FloatType(), True),
                    StructField("reg_mean_error", FloatType(), True),
                    StructField("date_key", StringType(), True),
                    StructField("reg_true_sales", FloatType(), True),
                    StructField("reg_pred_sales", FloatType(), True),
                    StructField("reg_pred_baseline", FloatType(), True),
                ]
            ),
        )
        print(regress_df.head(10))
        regress_df.write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\mregress_df.csv')


        extra_info_df = (
            df.select(
                "format_name",
                "item_segment",
                "item_main_category_code",
                "product_key",
                "date_key",
                "item_category_code",
                "number_stores_on_promo",
                "frac_stores_on_promo",
                "promo_price",
                "unpromo_price",
            )
            .withColumnRenamed("number_stores_on_promo", "reg_number_stores_on_promo")
            .withColumnRenamed("frac_stores_on_promo", "reg_frac_stores_on_promo")
        )
        print("extra_info_df counts rows :{}".format(extra_info_df.count()))

        return regress_df.join(
            extra_info_df,
            on=[
                "format_name",
                "item_segment",
                "item_main_category_code",
                "product_key",
                "date_key",
            ],
        )

    def calculate_fraction_stores_on_promo(self, df):

        return (
            df.groupBy(
                [
                    "format_name",
                    "item_segment",
                    "item_main_category_code",
                    "product_key",
                    "date_key",
                    "item_category_code",
                ]
            )
            .agg(
                F.count("store_key").alias("number_stores"),
                F.sum("quantity_total").alias("quantity_total"),
                F.sum("amount_price").alias("amount_price"),
                F.sum("amount_discount").alias("amount_discount"),
                F.sum("quantity_discount").alias("quantity_discount"),
                F.sum("on_promo").alias("number_stores_on_promo"),
                # features added
                F.max("f_holiday").alias("f_holiday"),
                F.max("ALPHA_PRC_EVENT_CD").alias("ALPHA_PRC_EVENT_CD"),
                F.avg("no_days_since_promo").alias("no_days_since_promo"),
                F.sum("no_stores_sell_product").alias("no_stores_sell_product"),
                F.avg("footfall_idx_cat").alias("footfall_idx_cat")
                ####F.avg('footfall_idx_cat').alias('footfall_idx_cat'),
                ####*aggs_max
            )
            .withColumn("date_key", df.date_key.astype("int"))
            .withColumn("product_key", df.product_key.astype("bigint"))
            .withColumn(
                "frac_stores_on_promo",
                F.col("number_stores_on_promo") / F.col("number_stores"),
            )
            .drop("number_stores")
        )

    def calculate_fraction_sold_on_promo(self, df):
        return df.withColumn(
            "frac_sold_on_promo", F.col("quantity_discount") / F.col("quantity_total")
        ).fillna(0)

    def calculate_averge_cost_per_product(self, df):
        return (
            df.withColumn(
                "av_price_per_sku", F.col("amount_price") / F.col("quantity_total")
            )
            .fillna(0)
            .withColumn(
                "av_discount_per_sku",
                F.col("amount_discount") / F.col("quantity_discount"),
            )
            .fillna(0)
        )

    def calculate_promo_nopromo_price(self, df):
        non_promo_prices_df = (
            ####df.filter(F.col("number_stores_on_promo") < 10) # Promos run on < 10 stores not counted in our analysis
            df.filter(
                F.col("number_stores_on_promo") < 1
            )  # Promos run on < 1 stores not counted in our analysis
            #促销活动在我们的分析中未计算的<1家商店上运行
            .groupBy(
                "item_segment", "item_main_category_code", "product_key", "format_name"
            )
            .agg(F.avg("av_price_per_sku").alias("avg_nonpromo_price"))
        )

        return (
            df.join(
                non_promo_prices_df,
                on=[
                    "item_segment",
                    "item_main_category_code",
                    "product_key",
                    "format_name",
                ],
                how="left",
            )
            .withColumn(
                "unpromo_price",
                F.udf(lambda n, p1, p2: p1 if n >= 1 else p2, FloatType())(
                    "number_stores_on_promo", "avg_nonpromo_price", "av_price_per_sku"
                ),
            )
            .withColumn(
                "promo_price", F.col("av_price_per_sku") - F.col("av_discount_per_sku")
            )
            .withColumn(
                "promo_depth",
                (F.col("unpromo_price") - F.col("promo_price"))
                / F.col("unpromo_price"),
            )
            .fillna(0)
        )

    def calculate_price_z_scores(self, df):
        # Assign temporary promo flag 分配临时促销标志
        res = df.withColumn(
            "promo_flag",
            (F.col("number_stores_on_promo") >= self.NB_STORE_LOWER_BOUND).astype(
                "int"
            ),
        )
        cols = ["unpromo_price", "promo_price"]
        ####stats = res.groupBy("format_name", "promo_flag").agg(*[
        ####  f(c).alias("{}_{}".format(c, f.__name__)) for c in cols for f in [F.avg, F.stddev_pop]
        ####])
        stats = res.groupBy("format_name", "promo_flag").agg(
            *[
                f(c).alias("{}_{}".format(c, f.__name__))
                for c in cols
                for f in [F.avg, F.stddev_pop]
            ]
        )
        # Get z-scores for promo prices 获取促销价格的z分数

        exprs = [
            (
                (F.col(c) - F.col("{}_avg".format(c)))
                / F.col("{}_stddev_pop".format(c))
            ).alias("{}_zscore".format(c))
            for c in cols
        ]

        res = res.join(
            F.broadcast(stats), on=["format_name", "promo_flag"], how="left"
        ).select(["*"] + exprs)

        # Make sure on non_promo days, the z_scores for promo prices are zero
        res = res.withColumn(
            "promo_price_zscore",
            F.when(F.col("promo_flag") == 0, 0).otherwise(F.col("promo_price_zscore")),
        )

        return res

    def make_date_related_features(self, df):

        # Instead of having a value, e.g. month = 10,
        # make one variable for month and have it as 0/1
        # i.e. month_1=0, ..., month_10=1
        day_of_week_opts = [i for i in range(7)]
        day_of_month_opts = [i for i in range(1, 32)]
        month_of_year_opts = [i for i in range(1, 13)]
        week_of_year_opts = [i for i in range(1, 53)]

        # Convert date_key from int to date_time format
        # Create day of the week, month and week from the date column
        res = (
            df.withColumn("date", F.to_date(df.date_key.astype("string"), "yyyyMMdd"))
            .withColumn("year", F.year("date"))
            .withColumn("day_of_week", (F.date_format("date", "u") - 1).astype("int"))
            .withColumn("day_of_month", F.dayofmonth("date"))
            .withColumn("month", F.month("date"))
            .withColumn("week", F.weekofyear("date"))
            .withColumn("first_fortnight", F.col("day_of_month") <= 15)
            .withColumn("first_fortnight", F.col("first_fortnight").cast("int"))
        )

        # Convert day of the week dummies Day = 0 to 6
        dow_dummies = [
            F.when(F.col("day_of_week") == cat, 1)
            .otherwise(0)
            .alias("weekday_" + str(cat))
            for cat in day_of_week_opts
        ]

        # Convert Day of month dummies Day = 1 to 31
        dom_dummies = [
            F.when(F.col("day_of_month") == cat, 1)
            .otherwise(0)
            .alias("monthday_" + str(cat))
            for cat in day_of_month_opts
        ]

        # Convert Month of year dummies Months 1 to 12
        moy_dummies = [
            F.when(F.col("month") == cat, 1).otherwise(0).alias("month_" + str(cat))
            for cat in month_of_year_opts
        ]

        # Convert Week of Year into dummies Week = 1 to 52
        woy_dummies = [
            F.when(F.col("week") == cat, 1).otherwise(0).alias("week_" + str(cat))
            for cat in week_of_year_opts
        ]

        # Select dummy columns and drop date columns since dummies are created
        res = res.select("*", *dow_dummies + dom_dummies + moy_dummies + woy_dummies)

        return res

    def calculate_promo_length_days_into_promo(self, df):
        days_in_promo_rdd = (
            df.filter(F.col("number_stores_on_promo") >= self.NB_STORE_LOWER_BOUND)
            .rdd.map(
                lambda r: (
                    (
                        r.format_name,
                        r.item_segment,
                        r.item_main_category_code,
                        r.product_key,
                    ),
                    r.date_key,
                )
            )
            .groupByKey()
            .flatMap(BaselineRegressionBase.compute_days_into_promo_cb)
        )

        days_in_promo_df = self.spark_session.createDataFrame(
            days_in_promo_rdd,
            StructType(
                [
                    StructField("format_name", StringType(), True),
                    StructField("item_segment", StringType(), True),
                    StructField("item_main_category_code", StringType(), True),
                    StructField("product_key", LongType(), True),
                    StructField("date_key", LongType(), True),
                    StructField("promo_length", LongType(), True),
                    StructField("days_into_promo", LongType(), True),
                ]
            ),
        )
        res = df.join(
            days_in_promo_df,
            on=[
                "format_name",
                "item_segment",
                "item_main_category_code",
                "product_key",
                "date_key",
            ],
            how="left",
        )
        return res.na.fill(0, subset=["promo_length", "days_into_promo"])
