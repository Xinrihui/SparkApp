#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from collections import Counter

# API doc
# https://spark.apache.org/docs/latest/api/python/reference/index.html
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

import os

def run(data_dir):
    """
    统计各区域热门商品 Top3：
    热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。

    1.实现自定义聚合函数 city_remark

    ref:

    :return:
    """

    spark = SparkSession.builder.appName('hotProduct').getOrCreate()

    # 1.建立 city_info 表
    city_schema = StructType()\
        .add(StructField("city_id", IntegerType(), True)) \
        .add(StructField("city_name", StringType(), True)) \
        .add(StructField("area", StringType(), True))

    df_city = spark.read.csv(header=False, schema=city_schema, sep='\t', path=os.path.join(data_dir, 'city_info.txt'))

    df_city.show()
    df_city.createOrReplaceTempView("city_info")

    # 2.建立 product_info 表
    product_schema = StructType()\
        .add(StructField("product_id", IntegerType(), True)) \
        .add(StructField("product_name", StringType(), True)) \
        .add(StructField("extend_info", StringType(), True))

    df_product = spark.read.csv(header=False, schema=product_schema, sep='\t', path=os.path.join(data_dir, 'product_info.txt'))

    df_product.show()
    df_product.createOrReplaceTempView("product_info")

    # 3.建立 user_visit_action 表
    user_visit_schema = StructType() \
        .add("date", StringType(), True) \
        .add("user_id", IntegerType(), True) \
        .add("session_id", StringType(), True) \
        .add("page_id", IntegerType(), True) \
        .add("action_time", StringType(), True) \
        .add("search_keyword", StringType(), True) \
        .add("click_category_id", IntegerType(), True) \
        .add("click_product_id", IntegerType(), True) \
        .add("order_category_ids", StringType(), True) \
        .add("order_product_ids", StringType(), True) \
        .add("pay_category_ids", StringType(), True) \
        .add("pay_product_ids", StringType(), True) \
        .add("city_id", IntegerType(), True) \

    df_user_visit = spark.read.csv(header=False, schema=user_visit_schema, sep='\t', path=os.path.join(data_dir, 'user_visit_action.txt'))

    # df_user_visit.show()
    df_user_visit.createOrReplaceTempView("user_visit_action")


    spark.sql(
        '''
		select 
		t2.area,
		t2.city_name,
		t1.click_product_id,
		t3.product_name
		from 
		user_visit_action t1 
		join city_info t2 on t1.city_id = t2.city_id
		join product_info t3 on t1.click_product_id = t3.product_id
		where t1.click_product_id > -1
        '''
          ).createOrReplaceTempView("t4")

    def city_remark(arrs):
        """

        统计 arrs 中的不同城市的个数, 输出为 分布的比例

        :param arrs:
        :return:
        """

        cnt_city = {}
        total = 0

        for city in arrs:

            total += 1

            if city not in cnt_city:
                cnt_city[city] = 0
            else:
                cnt_city[city] += 1

        sorted_city = sorted(cnt_city.items(), key=lambda x:x[1], reverse=True)

        first = sorted_city[0]
        second = sorted_city[1]

        res_first = "{}:{}%".format(first[0], format(first[1]*100/total, '.1f'))
        res_second = "{}:{}%".format(second[0], format(second[1] * 100 / total, '.1f'))
        res_other = "{}:{}%".format("其他", format((total-first[1]-second[1]) * 100 / total, '.1f'))

        return res_first + ',' + res_second + ',' + res_other


    city_remark_udf = udf(city_remark, StringType()) # 定义 UDF, 指明函数返回值的类型
    spark.udf.register("city_remark_udf", city_remark_udf)  # 注册 UDF, 等下可以在SQL中直接使用


    spark.sql(
        '''
        select 
        
        area,
        product_name,
        count(*) as clickCnt,
        city_remark_udf(collect_list(city_name)) as city_remark,
        row_number() over (partition by area order by count(*) desc) as rk
        
        from t4
        group by area, product_name
        
        '''
    ).createOrReplaceTempView("t5")

    spark.sql(
        '''
        select 

        area,
        product_name,
        clickCnt,
        city_remark,
        rk

        from t5
        where rk <=3 

        '''
    ).show(100, False)  # 最多显示数据集中的100行并且不对字段做截断处理


#+----+------------+--------+----------------------------------+---+
# |area|product_name|clickCnt|city_remark                       |rk |
# +----+------------+--------+----------------------------------+---+
# |华东|商品_86     |371     |上海:16.2%,杭州:15.6%,其他:68.2%  |1  |
# |华东|商品_47     |366     |杭州:15.6%,青岛:15.3%,其他:69.1%  |2  |
# |华东|商品_75     |366     |上海:17.2%,无锡:15.3%,其他:67.5%  |3  |
# |西北|商品_15     |116     |西安:53.4%,银川:44.8%,其他:1.7%   |1  |
# |西北|商品_2      |114     |银川:52.6%,西安:45.6%,其他:1.8%   |2  |
# |西北|商品_22     |113     |西安:54.0%,银川:44.2%,其他:1.8%   |3  |
# |华南|商品_23     |224     |厦门:28.6%,深圳:24.1%,其他:47.3%  |1  |
# |华南|商品_65     |222     |深圳:27.5%,厦门:26.1%,其他:46.4%  |2  |
# |华南|商品_50     |212     |福州:26.9%,深圳:25.5%,其他:47.6%  |3  |
# |华北|商品_42     |264     |保定:24.6%,郑州:24.6%,其他:50.8%  |1  |
# |华北|商品_99     |264     |北京:23.9%,郑州:23.1%,其他:53.0%  |2  |
# |华北|商品_19     |260     |郑州:23.1%,保定:20.0%,其他:56.9%  |3  |
# |东北|商品_41     |169     |哈尔滨:34.9%,大连:34.3%,其他:30.8%|1  |
# |东北|商品_91     |165     |哈尔滨:35.2%,大连:32.1%,其他:32.7%|2  |
# |东北|商品_58     |159     |沈阳:37.1%,大连:31.4%,其他:31.4%  |3  |
# |华中|商品_62     |117     |武汉:50.4%,长沙:47.9%,其他:1.7%   |1  |
# |华中|商品_4      |113     |长沙:52.2%,武汉:46.0%,其他:1.8%   |2  |
# |华中|商品_57     |111     |武汉:54.1%,长沙:44.1%,其他:1.8%   |3  |
# |西南|商品_1      |176     |贵阳:35.2%,成都:35.2%,其他:29.5%  |1  |
# |西南|商品_44     |169     |贵阳:36.7%,成都:33.7%,其他:29.6%  |2  |
# |西南|商品_60     |163     |重庆:39.3%,成都:30.1%,其他:30.7%  |3  |
# +----+------------+--------+----------------------------------+---+




    spark.stop()


if __name__ == '__main__':


    data_dir = '../data/product_click_dataset'

    run(data_dir)
