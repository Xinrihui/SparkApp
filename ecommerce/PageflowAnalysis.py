#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


class UserVisitAction:

    def __init__(self,
                 date,
                 user_id,
                 session_id,
                 page_id,
                 action_time,
                 search_keyword,
                 click_category_id,
                 click_product_id,
                 order_category_ids,
                 order_product_ids,
                 pay_category_ids,
                 pay_product_ids,
                 city_id
                 ):
        self.date = date  # String 用户点击行为的日期
        self.user_id = user_id  # Long,  用户的ID
        self.session_id = session_id  # String,  Session的ID
        self.page_id = page_id  # Long,  某个页面的ID
        self.action_time = action_time  # String,  动作的时间点
        self.search_keyword = search_keyword  # String,  用户搜索的关键词
        self.click_category_id = click_category_id  #: Long,  某一个商品品类的ID
        self.click_product_id = click_product_id  # Long,  某一个商品的ID
        self.order_category_ids = order_category_ids  # String,  一次订单中所有品类的ID集合
        self.order_product_ids = order_product_ids  # String,  一次订单中所有商品的ID集合
        self.pay_category_ids = pay_category_ids  # String,  一次支付中所有品类的ID集合
        self.pay_product_ids = pay_product_ids  # String,  一次支付中所有商品的ID集合
        self.city_id = city_id  # Long 城市


def run(file_dir, page_ids=('1', '2', '3', '4', '5', '6', '7')):
    conf = SparkConf()
    conf.setMaster("local").setAppName("HotCategoryTop10Analysis")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)  # 120s

    sc = SparkContext(conf=conf)

    sliceNum = 8

    facts_table = sc.textFile(file_dir, sliceNum)

    facts_table.cache()

    page_ids = list(page_ids)

    flow_id_tuples = zip(page_ids, page_ids[1:])

    # print(list(flow_id_tuples))
    # [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]

    # 结构化 facts_table

    def f_map(line):

        arr = line.split('_')

        row = UserVisitAction(date=arr[0],
                              user_id=arr[1],
                              session_id=arr[2],
                              page_id=arr[3],
                              action_time=arr[4],
                              search_keyword=arr[5],
                              click_category_id=arr[6],
                              click_product_id=arr[7],
                              order_category_ids=arr[8],
                              order_product_ids=arr[9],
                              pay_category_ids=arr[10],
                              pay_product_ids=arr[11],
                              city_id=arr[12]
                              )

        return row

    def f_filter(row):

        if row.page_id in set(page_ids):
            return True

        else:
            return False

    action_data_rdd = facts_table.map(f_map).filter(f_filter)

    # action_data_rdd.cache() # TODO: 会触发 java.net.SocketException: Connection reset by peer: socket write error

    # print(action_data_rdd.take(10))

    # 1.计算单跳转换率的分母
    pageid_count_rdd = action_data_rdd \
        .map(lambda row: (row.page_id, 1)) \
        .reduceByKey(lambda x, y: x + y)

    # print(pageid_count_rdd.take(10))

    pageid_count_dict = { ele[0]:ele[1] for ele in pageid_count_rdd.collect() }

    print(pageid_count_dict)

    # 2.计算单跳转换率的分子
    session_rdd = action_data_rdd.map(lambda row: (row.session_id, row.action_time, row.page_id)) \
        .groupBy(lambda x: x[0])

    # print(session_rdd.take(10))
    # [('07851728-f4ba-454b-a6ea-eb1855000ac5', < pyspark.resultiterable.ResultIterable object at 0x000002113BBE3608 >),

    def f_mapvalues(results):

        results = list(results)

        sort_results = sorted(results, key=lambda x: x[1])

        page_ids = [tp[2] for tp in sort_results]

        flow_id_tuples = zip(page_ids, page_ids[1:])

        # TODO：过滤不合法的

        return flow_id_tuples

    jump_count_rdd = session_rdd.mapValues(f_mapvalues).map(lambda x: x[1]) \
        .flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    print(jump_count_rdd.take(10))

    def f_foreach(jump_tuples):

        print(jump_tuples) # 在 executor 端打印, 在 driver 端看不到

    # jump_count_rdd.foreach(f_foreach)

    def f_map2(jump_tuples):

        (pageid1, pageid2), count = jump_tuples

        if pageid1 in pageid_count_dict:
            return (pageid1, pageid2), count/pageid_count_dict[pageid1]

        else:
            return (pageid1, pageid2), 0.0

    # 3.
    jump_rate = jump_count_rdd.map(f_map2)

    print(jump_rate.collect())

if __name__ == '__main__':
    data_dir = '../data/'
    file_dir = os.path.join(data_dir, 'user_visit_action.txt')
    run(file_dir)
