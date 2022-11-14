import pyspark
from pyspark.sql import types as spark_types

from promo_analytics.examples.data import SemanticSparkDataFrame

F_SKU_ID = "product_key"  # SKU ID
T_SKU_ID = spark_types.StringType()
####T_SKU_ID = spark_types.LongType()

F_STORE_FORMAT = (
    "format_name"
)  # store format or tyoe e.g. hypermarket, cornershop, neighbourhood
T_STORE_FORMAT = spark_types.StringType()

F_PROMO_ID = "promotion_key"  # promotion id (N/A means not a promotion)
T_PROMO_ID = spark_types.StringType()

F_STORE_ID = "store_key"  # Store ID
T_STORE_ID = spark_types.StringType()
####T_STORE_ID = spark_types.LongType()

F_H4_MAIN_CATEGORY_CODE = (
    "item_main_category_code"
)  # product heirachy: least granular level
T_H4_MAIN_CATEGORY_CODE = spark_types.StringType()
####T_H4_MAIN_CATEGORY_CODE = spark_types.LongType()

F_H4_MAIN_CATEGORY = "item_main_category"  # ...description
T_H4_MAIN_CATEGORY = spark_types.StringType()

F_H3_CATEGORY_CODE = "item_category_code"  # product heirachy: intermediate level
T_H3_CATEGORY_CODE = spark_types.StringType()
####T_H3_CATEGORY_CODE = spark_types.LongType()

F_H2_SUB_CATEGORY_CODE = "item_sub_category_code"
T_H2_SUB_CATEGORY_CODE = spark_types.StringType()
####T_H2_SUB_CATEGORY_CODE = spark_types.LongType()

F_H3_CATEGORY = "item_category"  # ....description
T_H3_CATEGORY = spark_types.StringType()

F_H2_SUB_CATEGORY = "item_sub_category"  # product heirachy: more granular level
T_H2_SUB_CATEGORY = spark_types.StringType()

F_H1_SEGMENT = "item_segment"  # product heirachy: more granular level
T_H1_SEGMENT = spark_types.StringType()

F_DATE_ID = "date_key"  # Date flag in YYYYmmdd format
T_DATE_ID = spark_types.LongType()

F_H5_DIVISION = "item_division"
T_H5_DIVISION = spark_types.StringType()

F_H5_DIVISION_CODE = "item_division_code"
T_H5_DIVISION_CODE = spark_types.StringType()
####T_H5_DIVISION_CODE = spark_types.LongType()

F_H1_SEGMENT_CODE = "item_segment_code"
T_H1_SEGMENT_CODE = spark_types.StringType()
####T_H1_SEGMENT_CODE = spark_types.LongType()


class MassDailySalesData(SemanticSparkDataFrame):

    # TODO: fix doctring
    # TODO: explain what the field names are

    """
    mass dailys is the sales volume of mass sales (non targeted) each day, blab blah
    """

    F_FORMAT_NAME = F_STORE_FORMAT  # see above
    F_SKU_ID = F_SKU_ID  # see above
    F_STORE_ID = F_STORE_ID  # see above
    F_DATE_ID = F_DATE_ID  # see above
    F_PROMO_ID = F_PROMO_ID  # see above
    F_H4_MAIN_CATEGORY_CODE = F_H4_MAIN_CATEGORY_CODE  # see above
    F_H4_MAIN_CATEGORY = F_H4_MAIN_CATEGORY  # see above
    F_H3_CATEGORY_CODE = F_H3_CATEGORY_CODE  # see above
    F_H3_CATEGORY = F_H3_CATEGORY  # see above
    F_H2_SUB_CATEGORY = F_H2_SUB_CATEGORY  # see above
    F_H1_SEGMENT = F_H1_SEGMENT  # see above
    F_QUANTITY_TOTAL = "quantity_total"  # see baseline
    F_AMOUNT_PRICE = "amount_price"  # see baseline
    F_AMOUNT_PRICE_EX_VAT = "amount_price_ex_vat"  # see baseline
    F_AMOUNT_DISCOUNT = "amount_discount"  # see baseline
    F_QUANTITY_DISCOUNT = "quantity_discount"  # see baseline

    _schema = spark_types.StructType(
        [
            spark_types.StructField(F_FORMAT_NAME, T_STORE_FORMAT, True),
            spark_types.StructField(F_SKU_ID, T_SKU_ID, True),
            spark_types.StructField(
                F_H4_MAIN_CATEGORY_CODE, T_H4_MAIN_CATEGORY_CODE, True
            ),
            spark_types.StructField(F_H4_MAIN_CATEGORY, T_H4_MAIN_CATEGORY, True),
            spark_types.StructField(F_H3_CATEGORY_CODE, T_H3_CATEGORY_CODE, True),
            spark_types.StructField(F_H3_CATEGORY, T_H3_CATEGORY, True),
            spark_types.StructField(F_H2_SUB_CATEGORY, T_H2_SUB_CATEGORY, True),
            spark_types.StructField(F_H1_SEGMENT, T_H1_SEGMENT, True),
            spark_types.StructField(F_DATE_ID, T_DATE_ID, True),
            spark_types.StructField(F_STORE_ID, T_STORE_ID, True),
            spark_types.StructField(F_QUANTITY_TOTAL, spark_types.DoubleType(), True),
            spark_types.StructField(F_AMOUNT_PRICE, spark_types.DoubleType(), True),
            spark_types.StructField(F_AMOUNT_DISCOUNT, spark_types.DoubleType(), True),
            spark_types.StructField(
                F_QUANTITY_DISCOUNT, spark_types.DoubleType(), True
            ),
            spark_types.StructField(F_PROMO_ID, T_PROMO_ID, True),
            spark_types.StructField(
                F_AMOUNT_PRICE_EX_VAT, spark_types.DoubleType(), True
            ),
        ]
    )

    _keys = (F_STORE_ID, F_SKU_ID, F_DATE_ID, F_PROMO_ID)
    ####def run_checks(self, df:pyspark.sql.DataFrame)-> None:
    def run_checks(self, df):
        self.keys_ok(df)
        # sales always positive
        # no na values
        # ....
        # day-storeformat-sku uniquely defines rows
        # pass
