#!/usr/bin/python
# -*- coding: UTF-8 -*-


from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def test_reducebykey():

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_reducebykey")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])

    print(sorted(rdd.reduceByKey(lambda x,y: x+y).collect()))


if __name__ == '__main__':

    test_reducebykey()