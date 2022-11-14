class SemanticSparkDataFrame:
    """
    abstract class - a wrapper around a spark dataframe that also has an expected schema
    """

    __slots__ = [
        "_schema",  # the expected schema for the spark dataframe
        "_keys",
    ]  # tuple of column names that uniquely define rows in the dataframe

    ####def __init__(self, spark_df : pyspark.sql.DataFrame) -> None:
    def __init__(self, spark_df):

        # matches_schema(self._schema, spark_df.schema)
        self._df = spark_df

    @property
    ####def df(self)-> pyspark.sql.DataFrame:
    def df(self):
        # return self._df.copy()
        return self._df

    @property
    ####def schema(self)-> spark_types.StructType():
    def schema(self):
        return self._schema

    @property
    ####def keys(self)-> typing.Tuple[str,]:
    def keys(self):
        return self._keys

    @classmethod
    ####def get_schema(cls)-> spark_types.StructType():
    def get_schema(cls):
        return cls._schema

    @classmethod
    ####def get_keys(cls) -> typing.Tuple[str,]:
    def get_keys(cls):
        return cls._keys

    # @abc.abstractmethod
    # ####def run_checks(self, spark_df : pyspark.sql.DataFrame):
    # def run_checks(self, spark_df):
    #     raise NotImplementedError('need to create your own if you want it')
    ####def keys_ok(self, spark_df : pyspark.sql.DataFrame):
    def keys_ok(self, spark_df):
        count1 = spark_df.select(self.keys).distinct().count()
        count2 = spark_df.count()
        ####assert count1 == count2, f'it is not unique at {self.keys}. The unique count is {count1} however total count is {count2}'
