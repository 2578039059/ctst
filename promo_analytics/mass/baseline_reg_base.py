#! /usr/bin/env python

from .model import ModelSpark

# start_date_str = "20180101"
# end_date_str = "20190831"


class Promo(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    @property
    def length(self):
        return (self.end - self.start).days + 1

    def into_promo(self, d):
        return (d - self.start).days + 1

    def __repr__(self):
        return "{} days promo from {}".format(self.length, self.start)


import datetime

todate = lambda i: datetime.datetime.strptime(str(i), "%Y%m%d")


class BaselineRegressionBase(ModelSpark):
    """Base class for baseline regression"""

    def __init__(self):
        self.promo_related_features = [
            "frac_stores_on_promo",
            "number_stores_on_promo",
            "frac_sold_on_promo",
            "promo_depth",
        ]
        #'promo_price', 'promo_price_zscore']
        self.nb_promo_related_features = len(self.promo_related_features)
        self.non_promo_related_features = ["unpromo_price", "unpromo_price_zscore"]
        self.non_promo_related_features += ["year"]
        self.non_promo_related_features += ["weekday_" + str(x) for x in range(0, 7)]
        self.non_promo_related_features += ["week_" + str(x) for x in range(1, 53)]
        self.non_promo_related_features += ["first_fortnight"]

        # ADDED NEW COLUMNS
        self.non_promo_related_features += [
            "f_holiday",
            "no_days_since_promo",
            "no_stores_sell_product",
            "footfall_idx_cat",
        ]
        print('1')

        self.reg_features = (
            self.promo_related_features + self.non_promo_related_features
        )
        print(self.reg_features)
        ####self.NB_STORE_LOWER_BOUND = 10
        self.NB_STORE_LOWER_BOUND = 1

    def make_features(self, df):
        print("res input data is :{}".format(df.head(2)))
        print("res input data rows :{}".format(df.count()))

        res = self.calculate_fraction_stores_on_promo(df)
        res.cache()
        # res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_1.csv')

        print("fraction_stores_on_promo data is :{}".format(res.head(2)))
        print(
            "=============================fraction_stores_on_promo data rows :{}".format(
                res.count()
            )
        )

        # Write recipe outputs
        # reduced_worker_test2 = dataiku.Dataset("reduced_worker_test2")
        # dkuspark.write_with_schema(reduced_worker_test2, res)

        # res = self.calculate_promo_length_days_into_promo(res)
        res = self.calculate_fraction_sold_on_promo(res)
        # res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_2.csv')
        res = self.calculate_averge_cost_per_product(res)
        # res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_3.csv')
        res = self.calculate_promo_nopromo_price(res)
        # res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_4.csv')
        res = self.calculate_price_z_scores(res)
        # res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_5.csv')
        res = self.make_date_related_features(res)
        res.repartition(1).write.option('header','true').csv('C:\\Users\Administrator\Documents\cx_test\make_features_6.csv')
        # added: data
        print("res data is :{}".format(res.head(100)))
        print("res data rows :{}".format(res.count()))

        # added steps for data cleaning
        # TODO: commented out by AG
        # res = res.filter(res.date_key >= start_date_str)
        # res = res.filter(res.date_key <= end_date_str)
        # res.persist()
        # print("filter one:{}".format(res.count()))
        res = res.filter(res.quantity_total > 0)
        print("filter two:{}".format(res.count()))
        # res = res.withColumn('quantity_total',log(res.quantity_total))
        # print("res data after log is :{}".format(res.head(2)))


        return res
