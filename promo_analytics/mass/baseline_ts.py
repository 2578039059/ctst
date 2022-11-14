#! /usr/bin/env python

import sys
from promo_analytics.mass.model import ModelSpark
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *


import datetime

todate = lambda i: datetime.datetime.strptime(str(i), "%Y%m%d")


class BaselineTimeSeriesSpark(ModelSpark):
    """Time series model for mass baseline

    The :meth:`run` method takes as argument the aggretation data for mass
    promos and return the baseline dataframe
    """
    """
    大规模基线的时间序列模型：
    meth：`run`方法将大规模促销的聚合数据作为参数，并以dataframe形式返回基线数据
    """

    def __init__(self, spark_session):
        self.NB_STORE_LOWER_BOUND = 1 # 下文中提到有额外的促销限制：在特定日期，至少有10家商店在促销中销售该商品，此处设定其实只有1家
        self.spark = spark_session

    def fit(self, df):
        w_baseline = self.est_time_series_baseline(df)
        RSQ = self.get_r_squared(w_baseline)

        return w_baseline.join(
            RSQ,
            on=[
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
            ],
        )

    def make_features(self, df):
        print("逻辑上第二个运行的 make_features 方法")
        # aggregate data to "format_name", "product_key", "promotion_key", "date_key" level
        # 将数据汇总到“ format_name”，“ product_key”，“ promotion_key”，“ date_key”级别
        res = self.get_agg_data(df)
        res.show()
        res = self.create_promo_flag(res)
        res.show()
        # Convert melted df into one record per Format, SKU, day, form
        # 将每种格式，SKU，日期，格式的融化df转换为一条记录
        df_flat = self.flatten_matrix(res)
        df_flat.show()
        # Create enpty data frame for all days, skus and formats
        # 为所有日期、skus 和格式创建空数据框
        df_frame = self.create_empty_place_holder(df_flat)
        df_frame.show()
        # calculate seasonality index on week level
        # 计算周水平的季节性指数
        df_wk_idx = self.calculate_wkly_seasonality_idx(df)
        df_wk_idx.show()
        df_mth_idx = self.calculate_mth_idx(df)
        df_mth_idx.show()
        # Join empty placeholder with data to have data for all days, SKUs and format
        # 将空占位符与数据连接起来以获取所有日期、SKU 和格式的数据
        df_full = self.create_full_df(df_frame, df_flat, df_wk_idx, df_mth_idx)
        df_full.show()
        # Get the day of the week factor
        # 获取星期几因素
        res = self.day_of_the_week_factor(df_full)
        res_idx = self.interpolate_spark_seasonality(res)#得到维度：分公司、部类、大类、商品、年份、周份，度量：周季节因子
        res_idx = res_idx.select(
            F.col("format_name").alias("format_name_alias"),
            F.col("item_main_category_code").alias("item_main_category_code_alias"),
            F.col("item_segment").alias("item_segment_alias"),
            F.col("product_key").alias("product_key_alias"),
            F.col("yr").alias("yr_alias"),
            F.col("wk").alias("wk_alias"),
            F.col("wk_idx").alias("wk_idx"),
        )
        print("complete idx")
        res_merge = res.join(
            res_idx,
            [
                res.format_name == res_idx.format_name_alias,
                res.item_main_category_code == res_idx.item_main_category_code_alias,
                res.item_segment == res_idx.item_segment_alias,
                res.product_key == res_idx.product_key_alias,
                res.yr == res_idx.yr_alias,
                res.wk == res_idx.wk_alias,
            ],
            how="left",
        ).select(
            [
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "wd",
                "yr",
                "wk",
                "date_key",
                "Date",
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
                "GUSBL_rev_bline",
                "GUSBL_qty_bline",
                "Unprm_Qty",
                "Unprm_Rev",
                "prev_index",
                "post_index",
                "day_idx",
                "wk_idx",
                "mth_idx"
            ]
        )
        # res_test = (res.join(res_idx, on = ['format_name', 'item_main_category_code', 'item_segment', 'product_key', 'yr', 'wk'], how = 'left'))
        print("Finish make features")
        ####res_merge.show()
        return res_merge

    def get_agg_data(self, df):
        print("make_features中执行第一个函数   Running method: get_agg_data")
        return df.groupBy(
            [
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "promotion_key",
                "date_key",
            ]
        ).agg(
            F.countDistinct("store_key").alias("num_stores"),
            F.sum("quantity_total").alias("quantity_total"),
            F.sum("amount_price").alias("amount_price"),
            F.sum("amount_discount").alias("amount_discount"),
            F.sum("quantity_discount").alias("quantity_discount"),
            F.avg("footfall_count").alias("footfall_count"),
            # F.avg("mth_idx").alias("mth_idx")
        )

    def create_promo_flag(self, df):
        # Crete a Promo Flag
        # Non-promo condition when all 3 field values are 0
        # 创建促销标志
        # 当所有3个字段值均为0时为非促销条件
        print("Running method: create_promo_flag")
        on_promo = F.udf(lambda i: i == "on_promo", BooleanType())("promotion_key")
        # Additional promo constraint: At least 10 stores are selling that item on promo on a particular day
        # 额外的促销限制：在特定日期，至少有10家商店在促销中销售该商品，此处设定其实只有1家
        return df.withColumn(
            "promo_flag",
            (on_promo & (df.num_stores >= self.NB_STORE_LOWER_BOUND)).cast("int"),
        ).withColumn("promo_str", F.col("promo_flag").astype("string"))

    def flatten_matrix(self, df):
        # Function creates a table wih one record per format, date, SKU.
        # Promo and NonPromo Sales are in separate columns instead of in separe rows
        # "Unmelting" of matrix (in R terms) or "Unstacking" of matrix (in Pandas terms)
        # Values to be aggregated and flattened
        # 函数创建一个表，其中每个记录的格式，日期，SKU均为一条。
        # 促销和非促销销售位于单独的列中，而不是单独的行中
        # 矩阵的“解链”（以R表示）或矩阵的“解堆”（以Pandas表示）
        # 要汇总和展平的值
        print("Running method: flatten_matrix")
        value_cols = [
            "quantity_total",
            "amount_discount",
            "quantity_discount",
            "amount_price",
            "promo_flag",
            "num_stores",
        ]

        # Create columns for the flattened version of the table (for promo/non-promo sales)
        # 为表格的展平版本创建列（用于促销/非促销）
        promo_0 = [
            F.when(F.col("promo_str") == "0", F.col(i))
            .otherwise(0)
            .alias("{}_0".format(i))
            for i in value_cols
        ]
        promo_1 = [
            F.when(F.col("promo_str") == "1", F.col(i))
            .otherwise(0)
            .alias("{}_1".format(i))
            for i in value_cols
        ]

        # Values to be aggregated and flattened with _0 for non promo values and _1 for promo values
        # 要聚合和展平的值，非促销值为_0，促销值为_1
        value_cols_num = [
            "{}_{}".format(i, j)
            for i, j in zip(
                value_cols * 2, [0] * len(value_cols) + [1] * len(value_cols)
            )
        ]
        # Group
        group_exprs = [F.sum(i).alias(i) for i in value_cols_num]

        # Flatten matrix using select and sum
        df_flat = (
            df.select(
                [
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                    "date_key",
                ]
                + promo_0
                + promo_1
            )
            .groupBy(
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "date_key",
            )
            .agg(*group_exprs)
        )

        return df_flat

    def create_empty_place_holder(self, df):
        # Get data frame with SKUs
        print("Running method: create_empty_place_holder")
        dayOfWeek_dict = {
            0: "Mon",
            1: "Tue",
            2: "Wed",
            3: "Thu",
            4: "Fri",
            5: "Sat",
            6: "Sun",
        }
        all_dates = df.select("date_key").distinct()
        return (
            df.groupBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .agg(
                F.min("date_key").alias("start_date"),
                F.max("date_key").alias("end_date"),
            )
            .crossJoin(F.broadcast(all_dates))
            .filter(F.col("date_key") >= F.col("start_date"))
            .filter(F.col("date_key") <= F.col("end_date"))
            .withColumn("Date", F.udf(lambda k: todate(k), TimestampType())("date_key"))
            .withColumn("yr", F.udf(lambda d: d.year, LongType())("Date"))
            .withColumn("wk", F.udf(lambda d: d.isocalendar()[1], LongType())("Date"))
            .withColumn(
                "wd", F.udf(lambda d: dayOfWeek_dict[d.weekday()], StringType())("Date")
            )
            .drop("start_date", "end_date")
        )

    def create_full_df(self, df_frame, df_flat, df_wk_idx, df_mth_idx):
        # Merge empty place holder with full dataframe
        # df_full = df_flat
        # Find total sales quantity
        print("Running method: create_full_df")

        df_full = (
            df_frame.join(
                df_flat,
                on=[
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                    "date_key",
                ],
                how="left",
            ).join(
                df_mth_idx,
                on=[
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                    "date_key",
                ],
                how="left"
            ).fillna(0)
            .withColumn(
                "total_sales_quantity",
                F.col("quantity_total_0") + F.col("quantity_total_1"),
            )
            .withColumn(
                "GUSBL_rev_bline", F.col("amount_price_0") + F.col("amount_price_1")
            )
            .withColumn(
                "GUSBL_qty_bline", F.col("quantity_total_0") + F.col("quantity_total_1")
            )
            # -------Find Unpromoted Quantity -----------
            .withColumn("Unprm_Qty", F.col("quantity_total_0"))
            .withColumn("Unprm_Rev", F.col("amount_price_0"))
            # -------Make Unpromoted Quantity and Revenue NULL so it doesn't Skew Average -----------
        )

        df_full = df_full.withColumn("Unprm_Qty", F.col("quantity_total_0"))
        df_full = df_full.withColumn("Unprm_Rev", F.col("amount_price_0"))

        cond1 = F.col("promo_flag_1") == 1
        cond2 = F.col("Unprm_Qty") == 0

        df_full = df_full.withColumn(
            "Unprm_Qty", F.when(cond1 | cond2, None).otherwise(F.col("Unprm_Qty"))
        )
        df_full = df_full.withColumn(
            "Unprm_Rev", F.when(cond1 | cond2, None).otherwise(F.col("Unprm_Rev"))
        )

        # df_full.cache()
        print(df_full.count())
        df_wk_idx = df_wk_idx.select(
            "item_main_category_code",
            "item_segment",
            "product_key",
            "yr",
            "wk",
            "prev_index",
            "post_index",
        )
        # df_wk_idx.cache()
        print(df_wk_idx.count())
        df_full = df_full.join(
            df_wk_idx,
            ["item_main_category_code", "item_segment", "product_key", "yr", "wk"],
            "left",
        )
        df_full = df_full.fillna(1, subset=["prev_index", "post_index"])
        return df_full

    def day_of_the_week_factor(self, df):
        print("Running method: day_of_the_week_factor")
        # -------Find average sales on atypical weekday  -----------
        df_wd_avg = df.groupBy(
            [
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "wd",
            ]
        ).agg(F.avg("Unprm_Qty").alias("wd_avg"))
        df_day_avg = df.groupBy(
            ["format_name", "item_main_category_code", "item_segment", "product_key"]
        ).agg(F.avg("Unprm_Qty").alias("day_avg"))
        return (
            df.join(
                F.broadcast(df_wd_avg),
                on=[
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                    "wd",
                ],
                how="left",
            )
            .join(
                F.broadcast(df_day_avg),
                on=[
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                ],
                how="left",
            )
            .withColumn("day_idx", F.col("wd_avg") / F.col("day_avg"))
            .drop("wd_avg", "day_avg")
            .fillna(1, subset=["day_idx"])
        )

    def calculate_wkly_seasonality_idx(self, df):
        print("Running method: calculate_wkly_seasonality_idx")
        # Get data frame with SKUs

        df_wk_idx = (
            df.withColumn(
                "Date", F.udf(lambda k: todate(k), TimestampType())("date_key")
            )
            .withColumn("yr", F.udf(lambda d: d.year, LongType())("Date"))
            .withColumn("wk", F.udf(lambda d: d.isocalendar()[1], LongType())("Date"))
        )
        df_wk_idx = df_wk_idx.groupby(
            "item_segment",
            "item_main_category_code",
            "item_category_code",
            "product_key",
            "yr",
            "wk",
        ).agg(
            F.avg("quantity_total").alias("quantity_total"),
            F.avg("footfall_count").alias("footfall_count"),
            F.avg("promo_flag").alias("promo_flag"),
        )

        window = Window.partitionBy(
            "item_segment",
            "item_main_category_code",
            "item_category_code",
            "product_key",
        ).orderBy("yr", "wk")

        df_wk_idx = df_wk_idx.withColumn(
            "prev_footfall_count", F.lead("footfall_count", -1, 0).over(window)
        ).withColumn("post_footfall_count", F.lead("footfall_count", 1, 0).over(window))

        df_wk_idx = df_wk_idx.groupby(
            "item_segment",
            "item_main_category_code",
            "item_category_code",
            "product_key",
            "yr",
            "wk",
        ).agg(
            F.avg("footfall_count").alias("footfall_count"),
            F.avg("prev_footfall_count").alias("prev_footfall_count"),
            F.avg("post_footfall_count").alias("post_footfall_count"),
        )

        df_wk_idx = df_wk_idx.withColumn(
            "prev_index", F.col("footfall_count") / F.col("prev_footfall_count")
        ).withColumn(
            "post_index", F.col("footfall_count") / F.col("post_footfall_count")
        )

        df_wk_idx = df_wk_idx.select(
            "item_segment",
            "item_main_category_code",
            "item_category_code",
            "product_key",
            "yr",
            "wk",
            "prev_index",
            "post_index",
        )
        return df_wk_idx

    def est_time_series_baseline(self, df):
        print("Running method: est_time_series_baseline")
        # Combination of two different averages.
        # Average 1: 50% weight on last 3 days, 50% weight on all other days in last 30
        # Average 2: 95% weight on last 1 day, 5% weight on all other days in last 30
        # Use average 1 unless value is notably volatile, in which case use average 2.
        # 两种不同平均值的组合。
        # 平均1：最近3天的权重占50％，最近30天的其他所有权重占50％
        # 平均2：最近1天的权重为95％，最近30天的其他所有权重为5％
        # 除非平均值非常不稳定，否则请使用平均值1，在这种情况下，请使用平均值2。


        lookback_days = 30
        short_horz_1, short_horz_2 = 3, 1 # 分别是两种算法：最近3/1天
        weight_1, weight_2 = 0.5, 0.95 # 分别是两种算法：权重占50%/95%

        switch_thrshld = 0.1# 开关阈值（暂时不知道什么用处）

        t = df.withColumn(#生成一个新字段 Unprm_Qty_lag ，后面为生成逻辑
            "Unprm_Qty_lag",
            F.lead(F.col("Unprm_Qty"), -1).over(
                Window.partitionBy(#window实际上就是开窗函数，对数据进行划分，此处为根据 分公司、部类、大类，商品编码维度 划分，根据日期进行排序。
                    "format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                ).orderBy("date_key")
            ),
        )

        win_for_shifts = Window.partitionBy(
            "format_name", "item_main_category_code", "item_segment", "product_key"
        ).orderBy("date_key")

        t = t.withColumn(
            "lag_horz_1",
            F.lead(F.col("Unprm_Qty_lag"), short_horz_1 * (-1)).over(win_for_shifts),
        )
        t = t.withColumn(
            "lag_horz_2",
            F.lead(F.col("Unprm_Qty_lag"), short_horz_2 * (-1)).over(win_for_shifts),
        )

        # -------Find Moving Averages------------
        w1_1 = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-short_horz_1 + 1, 0)
        )
        w1_2 = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-lookback_days + short_horz_1 + 1, 0)
        )
        t = t.withColumn(
            "baseline_1",
            weight_1 * F.avg(t.Unprm_Qty_lag).over(w1_1)
            + (1 - weight_1) * F.avg(t.lag_horz_1).over(w1_2),
        )

        w2_1 = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-short_horz_2 + 1, 0)
        )
        w2_2 = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-lookback_days + short_horz_2 + 1, 0)
        )
        t = t.withColumn(
            "baseline_2",
            weight_2 * F.avg(t.Unprm_Qty).over(w2_1)
            + (1 - weight_2) * F.avg(t.lag_horz_2).over(w2_2),
        )

        win_for_week = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-6, 0)
        )
        t = t.withColumn("weekly_av", F.avg(t.Unprm_Qty).over(win_for_week))

        replace_flag = F.abs(t.baseline_1 - t.weekly_av) / t.baseline_1 > switch_thrshld
        t = t.withColumn(
            "baseline", F.when(replace_flag, t.baseline_2).otherwise(t.baseline_1)
        )

        # Fill the first few nans with unprmoted quantities
        t = t.withColumn(
            "baseline",
            F.when(t.baseline.isNotNull(), t.baseline).otherwise(t.Unprm_Qty),
        )

        # Find promo period
        find_when_promo = t["promo_flag_1"] == 1
        t = t.withColumn(
            "baseline", F.when(find_when_promo, None).otherwise(t.baseline)
        )

        # Interpolate NULLs
        t = t.withColumn(
            "date_time", F.udf(lambda k: todate(k), TimestampType())("date_key")
        )
        t = t.withColumn("baseline", self.interpolate_spark_trend(t, "baseline"))

        # Multiply with Day of the Week Index
        t = t.withColumn("baseline", t.baseline * t.day_idx * t.wk_idx)

        find_when_zero_sales = t["total_sales_quantity"] == 0
        t = t.withColumn(
            "baseline", F.when(find_when_zero_sales, 0).otherwise(t.baseline)
        )
        t = t.withColumnRenamed("baseline", "time_pred_baseline")
        t = t.withColumn("time_pred_baseline1", (1 - F.col("mth_idx")) * (F.col("quantity_total_0") + F.col("quantity_total_1")) + F.col("mth_idx") * F.col("time_pred_baseline"))

        return t.drop(
            "Unprm_Qty_lag",
            "lag_horz_1",
            "lag_horz_2",
            "baseline_1",
            "baseline_2",
            "weekly_av",
        )

    def interpolate_spark_seasonality(self, df):
        print("Running method: interpolate_spark_seasonality")

        df = df.groupby(
            [
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "yr",
                "wk",
            ]
        ).agg(
            F.max("prev_index").alias("prev_index"),
            F.max("post_index").alias("post_index"),
            (1 - F.max("promo_flag_1")).alias("baseline_flag_1"),#本周是否有促销的标记，有则0，无则1
        )
        df = df.withColumn(
            "wk_baseline",
            F.when(F.col("baseline_flag_1") == 0, None).otherwise(F.col("wk")),
        )#当本周有促销，则生成wk_baseline字段为空，否则为wk

        windowval = Window.partitionBy(
            "format_name", "item_main_category_code", "item_segment", "product_key"
        ).orderBy([F.col("yr"), F.col("wk")])#生成窗口，分区：分公司、部类、大类、商品编码，排序:年份、该年周排序
        pw = windowval.rowsBetween(Window.unboundedPreceding, -1)  # Window.currentRow #生成规则：在第一行到前一行之间。
        not_null_week = F.when(
            F.col("wk_baseline").isNotNull(), F.col("wk_baseline")#感觉这一步没有什么意义
        )  # type: Column
        df = df.withColumn(
            "prev_week", F.last(not_null_week, ignorenulls=True).over(pw)
        )#生成prev_week字段为每一周前wk_baseline最后一个不为空的周号（也就是返回前一个无促销的周号）

        df = df.withColumn("cum_sum", F.sum("baseline_flag_1").over(pw))
        #生成cum_sum字段：维度：分区：分公司、部类、大类、商品编码、年份、周号，含义：该商品从起始计算日到当前日前一周中无促销的周数

        prev_window = (
            Window.partitionBy(
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "cum_sum",
                "baseline_flag_1",#本周是否促销标记
            )
            .orderBy([F.col("yr"), F.col("wk")])
            .rowsBetween(Window.unboundedPreceding, 0)
        )#生成窗口：分区：分公司、部类、大类、商品编码、该商品从起始计算日到当前日前一周中无促销的周数、本周是否促销，排序：年份，周号，规则：分区排序后第一行到当前行
        df = df.withColumn("index_list", F.collect_list("prev_index").over(prev_window))
        #collect_list函数是对一列字段进行合并，返回一个数组
        #在此处，我们根据prev_window进行分区后，将每前一周的季节性因子压缩，返回了数组。
        # （1.因为已经到了商品维度，所以说，被分到一个区的数据，应该是相邻几周都连续促销，所以cum_sum相同，baseline_flag_1相同（为0，促销））
        # （2.所以如果中间有非促销周，会中断，因为cum_sum改变，那么分区中就只有一条数据（baseline_flag_1为0，非促销））
        def cum_product(list_idx):
            cum = 1
            for i in list_idx:
                cum = cum * i
            return cum

        df = df.withColumn("wk_idx", F.udf(lambda k: cum_product(k))("index_list"))# 字段wk_idx为该维度下，所有前一周季节性因子的累乘
        df = df.select(
            "format_name",
            "item_main_category_code",
            "item_segment",
            "product_key",
            "yr",
            "wk",
            "wk_idx",
        )# 到此为止，季节性因子就算完了：wk_idx（还需要了解下prev_index对于促销周和非促销周的逻辑）
        return df

    def interpolate_spark_trend(self, df, col_to_interp):

        w = Window.partitionBy(
            "format_name", "item_main_category_code", "item_segment", "product_key"
        ).orderBy("date_time")
        pw = w.rowsBetween(Window.unboundedPreceding, -1)  # Window.currentRow
        fw = w.rowsBetween(1, Window.unboundedFollowing)
        not_null_date = F.when(
            F.col(col_to_interp).isNotNull(), F.col("date_time")
        )  # type: Column

        pdate = F.last(not_null_date, ignorenulls=True).over(pw).alias("_pdate")
        fdate = F.first(not_null_date, ignorenulls=True).over(fw).alias("_fdate")
        pvalue = (
            F.last(F.col(col_to_interp), ignorenulls=True).over(pw).alias("_pvalue")
        )
        fvalue = (
            F.first(F.col(col_to_interp), ignorenulls=True).over(fw).alias("_fvalue")
        )

        ivalue_back_forward = F.coalesce(
            F.col(col_to_interp),
            (
                pvalue
                + F.datediff(F.col("date_time"), pdate)
                * (fvalue - pvalue)
                / F.datediff(fdate, pdate)
            ),  # type: Column
            # Bfill
            pvalue,
            # Ffill
            fvalue,
        ).alias(col_to_interp)

        return ivalue_back_forward

    def get_r_squared(self, df):
        res = df.withColumn(
            "time_pred_baseline",
            F.when(df.promo_flag_1 == 1, None).otherwise(df.time_pred_baseline),
        ).withColumn(
            "total_sales_quantity",
            F.when(df.promo_flag_1 == 1, None).otherwise(df.total_sales_quantity),
        )

        w = (
            Window.partitionBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .orderBy("date_key")
            .rowsBetween(-sys.maxsize, sys.maxsize)
        )

        res = (
            res.withColumn(
                "error",
                (F.col("total_sales_quantity") - F.col("time_pred_baseline")) ** 2,
            )
            .withColumn(
                "signal",
                (
                    F.col("total_sales_quantity")
                    - F.mean(F.col("total_sales_quantity")).over(w)
                )
                ** 2,
            )
            .withColumn("sum_error", F.sum("error").over(w))
            .withColumn("sum_signal", F.sum("signal").over(w))
        )

        R = (
            res.groupBy(
                "format_name", "item_main_category_code", "item_segment", "product_key"
            )
            .agg(
                F.first("sum_error").alias("sum_error_f"),
                F.first("sum_signal").alias("sum_signal_f"),
            )
            .withColumn(
                "time_r_squared", 1 - (F.col("sum_error_f") / F.col("sum_signal_f"))
            )
            .select(
                "format_name",
                "item_main_category_code",
                "item_segment",
                "product_key",
                "time_r_squared",
            )
            .withColumn(
                "time_r_squared",
                F.when(
                    F.col("time_r_squared").isNotNull(), F.col("time_r_squared")
                ).otherwise(0),
            )
        )

        return R

    def mth_index_adjust(self, df, col_name):

        df = df.withColumn(
            col_name,
            (1 - F.col("mth_idx")) * (F.col("quantity_total_0") + F.col("quantity_total_1")) + F.col("mth_idx") * F.col(col_name),
        )

        return df

    def calculate_mth_idx(self, df):

        df_out = df.groupBy("format_name",
                    "item_main_category_code",
                    "item_segment",
                    "product_key",
                    "date_key",).agg(F.avg("mth_idx").alias("mth_idx"))



        return df_out