from pyspark.sql.functions import monotonically_increasing_id


def get_stochastic_var(df, rate):

    df = df.withColumn("rate_idx", monotonically_increasing_id() % 1000)
    df = df.join(rate, df.rate_idx == rate.idx, how="left")
    df = df.drop(*["rate_idx", "idx"])

    return df
