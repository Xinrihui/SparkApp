#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.accumulators import AccumulatorParam

from enum import Enum

from pyspark.sql import SparkSession

def test_accumulator():

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_accumulator")
    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4])

    acc_sum = sc.accumulator(0)

    def f(x):
        acc_sum.add(x)

    rdd.foreach(f)

    print(acc_sum.value)

class VectorAccumulatorParam(AccumulatorParam):

    def zero(self, value):
        return [0.0] * len(value)

    def addInPlace(self, val1, val2):
        for i in range(len(val1)):
            val1[i] += val2[i]
        return val1

def test_VectorAccumulator():

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_accumulator")
    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)
    sc = SparkContext(conf=conf)

    va = sc.accumulator([1.0, 2.0, 3.0], VectorAccumulatorParam())
    print(va.value)

    def g(x):
        va.add([x] * 3)

    rdd = sc.parallelize([1, 2, 3])

    rdd.foreach(g)

    print(va.value)


class DictAccumulatorMethod(Enum):
    # REPLACE either adds a new key value or replace the value of existing key
    REPLACE = lambda d, key, value: \
        d.update({key: value})
    # KEEP either adds a new key value or keeps the value of existing key
    KEEP = lambda d, key, value: \
        d.update({key: d.get(key, value)})
    # ADD adds the value to existing value or add new key value
    ADD = lambda d, key, value: \
        d.update({key: (d[key] + value) if key in d else value})
    # LIST add new value to the list pertaining to given key
    LIST = lambda d, key, value: \
        d.update({key: d.get(key, []) + value})
    # SET add new value to the set pertaining to given key
    SET = lambda d, key, value: \
        d.update({key: list(set(d.get(key, set([]))).union(value))})


class DictAccumulator(AccumulatorParam):

    def __init__(self, dict_accumulator_method=DictAccumulatorMethod.REPLACE):
        """
        Initialize accumulator with specific type
        :param dict_accumulator_method: method which defines combining values
        based on type
        """
        # self.method = dict_accumulator_method
        pass

    def zero(self, init_value: dict):
        return init_value

    def addInPlace(self, v1: dict, v2: dict):
        for key, value in v2.items():
            self.method(v1, key, value)
        return v1

    def method(self, d, key, value):

        # d.update({key: value})
        d[key] = value

class AccumulatorManager:

    def __init__(self, acc):
        self.acc = acc

    def accumulator(self):
        return self.acc

def test_DictAccumulator():

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_accumulator")
    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([i for i in range(10)])

    acc = sc.accumulator(dict(), DictAccumulator(DictAccumulatorMethod.REPLACE))

    acc_manager = AccumulatorManager(acc)

    def process(x, acc_manager_inst):
        accumulator = acc_manager_inst.accumulator()
        accumulator.add({x: x})

    def process2(x):
        acc.add({x: 'res:'+str(x)})

    # rdd.foreach(lambda x: process(x, acc_manager))

    rdd.foreach(process2)

    out = acc.value

    print(out)


if __name__ == '__main__':

    test_DictAccumulator()



