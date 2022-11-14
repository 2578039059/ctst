import pyspark.sql.functions as F
from promo_analytics import get_stochastic_var, mth_index_adjust


def baseline_opt(df, col_name):

    new_col_name = col_name.replace("_raw", "")

    df = df.withColumn(
        new_col_name,
        F.when(
            F.col(col_name) >= F.col("sale_qty"),
            F.col("sale_qty") * (F.lit(1) - F.col("rate")),
        ).otherwise(F.col(col_name)),
    )

    return df


def exe(df, spark):

    rate = spark.sql(
        """
    SELECT *
    FROM lhBCGTest.rate_test
    """
    )

    col_in = [
        "company_code",
        "item_code",
        "c_code",
        "dt",
        "sale_qty",
        "ts_baseline_raw_no",
        "ts_baseline_r_square_no",
        "ts_baseline_raw_all",
        "ts_baseline_r_square_all",
        "ts_baseline_raw_dp",
        "ts_baseline_r_square_dp",
        "ts_baseline_raw_c",
        "ts_baseline_r_square_c",
        "ts_baseline_raw_group_fm",
        "ts_baseline_r_square_group_fm",
        "ts_baseline_raw_fm",
        "ts_baseline_r_square_fm",
        "is_elasticity",
        "elasticity_raw_baseline",
        "reg_raw_baseline",
        "reg_baseline_r_squrare",
        "promo_comment",
    ]

    df = df.select(*col_in)
    df = get_stochastic_var(df, rate)

    for col in [
        "ts_baseline_raw_no",
        "ts_baseline_raw_all",
        "ts_baseline_raw_dp",
        "ts_baseline_raw_c",
        "ts_baseline_raw_group_fm",
        "ts_baseline_raw_fm",
        "elasticity_raw_baseline",
        "reg_raw_baseline",
    ]:

        df = baseline_opt(df, col_name=col)

    df = df.drop("rate")

    ## temp step 2
    # mth_idx = spark.sql(
    #     """
    # SELECT *
    # FROM lhBCGTest.test_base
    # """
    # )
    #
    # mth_idx = mth_idx.withColumnRenamed("dt", "dt_temp").withColumnRenamed(
    #     "c_code", "c_code_temp"
    # )
    #
    # join_cond = [(df.dt == mth_idx.dt_temp) & (df.c_code == mth_idx.c_code_temp)]
    # df = df.join(mth_idx, join_cond, how="left").drop("c_code_temp", "dt_temp")
    #
    # for col in [
    #     "ts_baseline_no",
    #     "ts_baseline_all",
    #     "ts_baseline_dp",
    #     "ts_baseline_c",
    #     "ts_baseline_group_fm",
    #     "ts_baseline_fm",
    #     "elasticity_baseline",
    #     "reg_baseline",
    # ]:
    #     df = mth_index_adjust(df, col)

    spark.sql("DROP TABLE IF EXISTS lhsmartprom.model_baseline_est_step9_4")

    df.write.saveAsTable("lhsmartprom.model_baseline_est_step9_4")
