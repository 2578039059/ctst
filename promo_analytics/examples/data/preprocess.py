from pyspark.sql.types import *
import pyspark.sql.functions as F


def rename_cols(df, old_cols, new_cols):
    print('1')
    print(old_cols)
    print(new_cols)
    df.show()


    for old_col, new_col in zip(old_cols, new_cols):

        df = df.withColumnRenamed(old_col, new_col)

    return df


def prepare(df, spark):


    # spark.sql("DROP TABLE IF EXISTS lhBCGTest.df_input")
    # df.write.saveAsTable("lhBCGTest.df_input")
    spark.sql("TRUNCATE TABLE  lhsmartonline.backup_df_input")
    df.write.insertInto("lhsmartonline.backup_df_input")

    df_date = spark.sql(
        """
        SELECT DISTINCT ds AS dt
        FROM lhsmartprom.ods_pricing_calendar_sales_daily_item_step3 a
        WHERE a.is_weighted_flag <> 1
        AND a.in_scope_item_flag = 1
        """
    )#从销售日历中拿到“非散装”和“模型覆盖”的商品有销售的日期

    df_idx_param = spark.sql(
        """
        SELECT *
        FROM lhsmartprom.temp_time_index
        """
    )
    # 以下为提取出的数据
    # start_date	end_date	time_factor
    # 2018-01-01	2019-01-31	1.0
    # 2019-02-01	2020-10-31	1.0

    join_cond = [
        (df_date["dt"] >= df_idx_param["start_date"]) &
        (df_date["dt"] <= df_idx_param["end_date"])
    ]# 设定关联条件：大概就是圈定计算日期吧

    df_date = df_date.join(df_idx_param, join_cond, how="left").drop(
        *["start_date", "end_date"]
    )# 关联设定计算日期

    # spark.sql("DROP TABLE IF EXISTS lhBCGTest.test_df_date")
    # df_date.write.saveAsTable("lhBCGTest.test_df_date")#写入test_df_date表中的是所有计算日期
    spark.sql("TRUNCATE TABLE  lhsmartonline.backup_test_df_date")
    df_date.write.insertInto("lhsmartonline.backup_test_df_date")#写入test_df_date表中的是所有计算日期

    df_category = spark.sql(
        """
        SELECT DISTINCT c_code
        FROM lhsmartprom.olap_item_info
        """
    )# 提取所有的大类编码

    df_base = df_date.crossJoin(df_category)#得到的结果为：所有大类关联所有计算日期

    # spark.sql("DROP TABLE IF EXISTS lhBCGTest.test_df_base")
    # df_base.write.saveAsTable("lhBCGTest.test_df_base")# 写入test_df_base表的是所有大类关联所有计算日期
    spark.sql("TRUNCATE TABLE  lhsmartonline.backup_test_df_base")
    df_base.write.insertInto("lhsmartonline.backup_test_df_base")# 写入test_df_base表的是所有大类关联所有计算日期

    df_c_code = spark.sql(
        """
        SELECT
        DATE_ADD(start_date, b.day_no) AS dt
        , c_code
        , time_factor AS time_factor_c_code
        FROM temp_time_index_c_code
        LATERAL VIEW POSEXPLODE(SPLIT(CONCAT('|',SPACE(DATEDIFF(end_date, start_date)),'|'),' ')) b AS day_no,x"""
        # POSEXPLODE函数是EXPLODE函数的进阶版，多个POSEXPLODE组合可以通过切割的位置正确将新生成的不同列通过POSEXPLODE对应上。（通过生成表关联的方式）
        # as 后面分别是表名，字段名
        # 此处是生成空格给
    )

    df_mth_idx = df_base.join(df_c_code, on=["dt", "c_code"], how="left")
    df_mth_idx = df_mth_idx.withColumn("mth_idx", F.coalesce(F.col("time_factor_c_code"), F.col("time_factor")))

    df_mth_idx = df_mth_idx.select("dt", "c_code", "mth_idx")

    df_mth_idx = df_mth_idx.withColumnRenamed("dt", "dt_temp").withColumnRenamed(
        "c_code", "c_code_temp"
    )

    # spark.sql("DROP TABLE IF EXISTS lhBCGTest.test_df_mth_idx")
    # df_mth_idx.write.saveAsTable("lhBCGTest.test_df_mth_idx")
    spark.sql("TRUNCATE TABLE  lhsmartonline.backup_test_df_mth_idx")
    df_mth_idx.write.insertInto("lhsmartonline.backup_test_df_mth_idx")

    join_cond = [(df.date_key == df_mth_idx.dt_temp) & (df.c_code == df_mth_idx.c_code_temp)]
    df = df.join(df_mth_idx, join_cond, how="left").drop("c_code_temp", "dt_temp")

    # spark.sql("DROP TABLE IF EXISTS lhsmartprom.model_baseline_est_upgrade_step5")
    # df.write.saveAsTable("lhsmartprom.model_baseline_est_upgrade_step5")
    spark.sql("TRUNCATE TABLE lhsmartonline.backup_baseline_est_upgrade_step5")
    df.write.insertInto("lhsmartonline.backup_baseline_est_upgrade_step5")

    return df


def exe(df, category):

    cols_common = [
        "format_name",
        "product_key",
        "item_main_category_code",
        "item_main_category",
        "item_category_code",
        "item_category",
        "item_sub_category",
        "item_segment",
        "store_key",
        "quantity_total",
        "amount_price",
        "amount_discount",
        "quantity_discount",
        "promotion_key",
        "amount_price_ex_vat",
        "unit_cost_sum",
        "promo_flag",
        "f_holiday",
        "no_days_since_promo",
        "no_stores_sell_product",
        "footfall_count_cat",
        "footfall_idx_cat",
        "alpha_prc_event_cd",
        "date_key",
        "mth_idx"
    ]

    cols_var = ["footfall_count", "footfall_idx"]

    postfix = "_" + category

    cols_category = [col + postfix for col in cols_var]
    print(cols_category)
    df.show()
    df = df.select(*(cols_common + cols_category))
    df.show()
    print('1')
    df = rename_cols(df, cols_category, cols_var)
    print('1')
    df.show()
    return df
