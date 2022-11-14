from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as F


def get_stochastic_var(df, rate):

    df = df.withColumn("rate_idx", monotonically_increasing_id() % 1000)
    df = df.join(rate, df.rate_idx == rate.idx, how="left")
    df = df.drop(*["rate_idx", "idx"])

    return df


def mth_index_adjust(df, col_name):

    df = df.withColumn(
        col_name,
        (1 - F.col("mth_idx")) * F.col("sale_qty") + F.col("mth_idx") * F.col(col_name),
    )

    return df
