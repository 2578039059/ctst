#! /usr/bin/env python

####from abc import ABC, abstractmethod
from abc import abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.types import *

#这个类其实只是个抽象类，规定了实现这个抽象类的计算模型的执行调用顺序！！！
class Model:
    """Base class for models"""

    def run(self, df):
        """Main execution call, with 3 steps:
            * :meth:`preprocess`: Data preparation
            * :meth:`make_features`: Create the desired features
            * :meth:`fit`: Fit a model to the created features

        Args:
            df: The input data
        Return:
            Dataframe containing the results

        """
        """
        主要执行调用，分3个步骤：
             *：meth：`preprocess`：数据准备
             *：meth：`make_features`：创建所需的特征
             *：meth：`fit`：拟合，也就是用特征作训练了

         入参：
             df：输入数据
         返回：
             包含结果的数据框
        """
        clean_df = self.preprocess(df)# 调用处理方法
        print("The clean_df dataset input for regression :{}".format(clean_df.head(10)))# 提取clean_df的前10行数据，我感觉只是简单看下数据格式吧。
        features = self.make_features(clean_df)# 提取特征
        return self.fit(features) # 调用拟合方法

    @abstractmethod
    def make_features(self, df):
        pass

    @abstractmethod
    def fit(self, features):
        pass

    @abstractmethod
    def preprocess(self, df):
        pass


# class ModelPandas(Model):
#     def preprocess(self, df):
#
#         df = df.copy()
#
#         # Around 0.2% of entries have a negative quantity_total / amount price
#         # Ignore these
#         df = df[df["quantity_total"] >= 0]
#
#         df["quantity_discount"] = df["quantity_discount"].abs()
#         df["amount_discount"] = df["amount_discount"].abs()
#
#         df["on_promo"] = [
#             i is not None and len(i) > 0 and i != "N/A" and i != "not_on_promo"
#             for i in df.promotion_key
#         ]
#         df["on_promo"] = df["on_promo"].astype(int)
#         return df


class ModelSpark(Model):
    """Base class for mass processing in spark"""

    def preprocess(self, df):
        # Ignore 0.2% of entries have a negative quantity_total / amount price
        # 忽略0.2％的条目具有负的quantity_total /金额价格
        res = df.filter(F.col("quantity_total") >= 0)

        # Make the discount variabes positive, rather than negative
        # 使折扣变量为正，而不是为负
        res = res.withColumn("quantity_discount", F.abs(res.quantity_discount))
        res = res.withColumn("amount_discount", F.abs(res.amount_discount))

        ####promo_cond =  F.udf(lambda i: i is not None and len(i) > 0 and i!= "N/A", BooleanType())("promotion_key")
        promo_cond = F.udf(
            lambda i: i is not None
            and len(i) > 0
            and i != "N/A"
            and i != "not_on_promo",
            BooleanType(),
        )("promotion_key")
        # Create promo flag
        res = res.withColumn("on_promo", promo_cond.astype("int"))
        # res.write.csv('C:\\Users\Administrator\Documents\cx_test\preprocess_finsh.csv')
        print("逻辑上第一个运行的 preprocess 方法")
        return res
