#!/usr/bin/python
# -*- coding: UTF-8 -*-


# API doc
# https://spark.apache.org/docs/latest/api/python/reference/index.html

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.types import *

from pyspark.sql.functions import udf, pandas_udf, PandasUDFType, collect_list
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

import pandas as pd



def test_reducebykey():

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_reducebykey")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])

    print(sorted(rdd.reduceByKey(lambda x,y: x+y).collect()))

    sc.stop()

def test_df1():
    """
    测试 dataFrame

    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("test_df1")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    df = spark.read.json('../data/user.json')
    df.show()

    df.createOrReplaceTempView("people") # 注册表
    df1 = spark.sql("SELECT age FROM people where age >= 30")
    df1.show()

    df.filter(df.age >= 30).show()  # DSL

    sc.stop()
    spark.stop()

def test_df2():
    """
    测试 dataFrame

    RDD 与 DataFrame的相互转换
    :return:
    """

    spark = SparkSession.builder.appName('test_df2').getOrCreate()

    # RDD <=> DataFrame
    rdd = spark.sparkContext.parallelize([(1, "zhangsan", 30), (2, "lisi", 40)])
    columns = ["id", "name", "age"]
    df = rdd.toDF(columns)
    df.show()

    print(df.rdd.collect())
    # [Row(id=1, name='zhangsan', age=30), Row(id=2, name='lisi', age=40)]

    spark.stop()


def test_udf():
    """

    自定义函数

    ref:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html


    :return:
    """

    spark = SparkSession.builder.appName('test_udf').getOrCreate()

    slen = udf(lambda s: len(s), IntegerType())

    @udf
    def to_upper(s):
        if s is not None:
            return s.upper()

    @udf(returnType=IntegerType())
    def add_one(x):
        if x is not None:
            return x + 1

    df = spark.createDataFrame([(1, "John Doe", 21),(1, "Keven", 31)], ("id", "name", "age"))
    df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()

    df.createOrReplaceTempView("people")
    spark.udf.register("to_upper", to_upper)  # 注册UDF

    spark.sql("SELECT to_upper(name),age FROM people where age >= 30").show()

    spark.stop()

def test_udaf1():
    """

    自定义聚合函数 UDAF

    ref:
    https://spark.apache.org/docs/2.4.1/api/python/pyspark.sql.html?highlight=pandas_udf#pyspark.sql.functions.pandas_udf
    https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html#pyspark.sql.functions.pandas_udf

    https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html
    https://spark.apache.org/docs/2.4.0/sql-pyspark-pandas-with-arrow.html

    :return:
    """

    spark = SparkSession.builder.appName('test_udaf1').getOrCreate()

    df = spark.createDataFrame(
        [(1, 1.0),
         (1, 2.0),
         (2, 3.0),
         (2, 5.0),
         (2, 10.0)],
        ("id", "v"))

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(v):
        return v.mean()

    df.groupby("id").agg(mean_udf(df['v'])).show()

    spark.stop()


def test_udaf2():
    """

    自定义聚合函数

    ref:

    https://danvatterott.com/blog/2018/09/06/python-aggregate-udfs-in-pyspark/

    :return:
    """

    spark = SparkSession.builder.appName('test_udaf2').getOrCreate()

    df = spark.createDataFrame(
        [[1, 'a'],
         [1, 'b'],
         [1, 'b'],
         [2, 'c']],
        ['id', 'value'])

    df.createOrReplaceTempView("t1")
    df.show()

    df.groupBy('id').agg(collect_list('value').alias('value_list')).show()

    spark.sql(
        """
        select
        id,
        collect_list(value) as value_list
        
        from t1
        group by id  
    
        """
    ).show()

    def count_a(arrs):
        """
        记录 collect_list 收上来的 arrs 中含有多少个 'a'

        :param arrs:
        :return:
        """

        cnt = 0

        for i in arrs:
            if i == 'a':
                cnt += 1

        return cnt

    count_a_udf = udf(count_a, IntegerType()) # 定义 UDF, 指明函数返回值的类型

    spark.udf.register("count_a_udf", count_a_udf)  # 注册 UDF, 等下可以在SQL中直接使用

    df.groupBy('id').agg(count_a_udf(collect_list('value')).alias('a_count')).show()

    spark.sql(
        """
        select
        id,
        count_a_udf(collect_list(value)) as a_count

        from t1
        group by id  

        """
    ).show()

    spark.stop()


if __name__ == '__main__':

    # test_reducebykey()

    test_udaf2()
