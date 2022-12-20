#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os

from operator import add

from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.accumulators import AccumulatorParam


def run(file_dir):
    conf = SparkConf()
    conf.setMaster("local").setAppName("HotCategoryTop10Analysis")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    sliceNum = 8

    facts_table = sc.textFile(file_dir, sliceNum)

    facts_table.cache()

    # print(facts_table.take(10))

    # 1.计算品类的点击数量

    count_category_rdd = facts_table.map(lambda line:
                                         line.split('_')[6]
                                         ).filter(lambda x: x != "-1").map(lambda x: (x, 1)).reduceByKey(add)

    # print(count_category_rdd.take(3))

    # 2.计算品类的下单数量

    # map: 一行 返回一个
    # flatMap: 一行返回 list 由 flatMap 自动把 list 扁平化成 ele

    count_order_rdd = facts_table.map(lambda line:
                                      line.split('_')[8]
                                      ).filter(lambda x: x != 'null').flatMap(lambda ids: ids.split(',')). \
        map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    # print(count_order_rdd.take(3))
    # [('16', 1782), ('4', 1760), ('20', 1776)]

    # 3.计算品类的支付数量
    count_pay_rdd = facts_table.map(lambda line:
                                    line.split('_')[10]
                                    ).filter(lambda x: x != 'null').flatMap(lambda ids: ids.split(',')). \
        map(lambda x: (x, 1)).reduceByKey(add)

    # print(count_pay_rdd.take(3))

    # 4. 将品类进行排序，并且取前10名
    # 点击数量排序，下单数量排序，支付数量排序
    # 元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    # ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    #
    # 3表关联 但是 每张表的 某个品类ID 皆有可能为空, 只能选择
    # (1) cogroup = connect + group
    # (2) full outer join

    category_order_rdd = count_category_rdd.fullOuterJoin(count_order_rdd)

    # TODO: 多表关联不能直接写在后面, 而是要拆开分别去关联
    # print(category_order_rdd.take(3))
    # [('16', (5928, 1782)), ('12', (6095, 1740)), ('4', (5961, 1760))]

    category_order_pay_rdd = category_order_rdd.fullOuterJoin(count_pay_rdd)

    # print(category_order_pay_rdd.take(3))
    # [('16', ((5928, 1782), 1233)), ('12', ((6095, 1740), 1218)), ('4', ((5961, 1760), 1271))]

    def f3(values):

        category_order, pay = values

        category, order = category_order

        count_c, count_o, count_p = category, order, pay

        if category is None:
            count_c = 0

        if order is None:
            count_o = 0

        if pay is None:
            count_p = 0

        return (count_c, count_o, count_p)

    category_order_pay_rdd2 = category_order_pay_rdd.mapValues(f3)

    # print(category_order_pay_rdd2.take(3))
    # [('16', (5928, 1782, 1233)), ('12', (6095, 1740, 1218)), ('4', (5961, 1760, 1271))]

    result = category_order_pay_rdd2.sortBy(lambda x: x[1], ascending=False)

    print(result.take(10))
    # [('15', (6120, 1672, 1259)), ('2', (6119, 1767, 1196)), ('20', (6098, 1776, 1244)), ('12', (6095, 1740, 1218)), ('11', (6093, 1781, 1202)), ('17', (6079, 1752, 1231)), ('7', (6074, 1796, 1252)), ('9', (6045, 1736, 1230)), ('19', (6044, 1722, 1158)), ('13', (6036, 1781, 1161))]


def run_v2(file_dir):
    """
    reducebykey 和 Join 有可能会触发 shuffle 操作, 我们要减少它们的使用

    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("HotCategoryTop10Analysis")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)  # 120s

    sc = SparkContext(conf=conf)

    sliceNum = 8

    facts_table = sc.textFile(file_dir, sliceNum)

    # facts_table.cache()

    # print(facts_table.take(10))

    # reducebykey 和 Join 有可能会触发 shuffle 操作, 我们要减少它们的使用

    # 1.计算 品类的点击数量, 下单数量, 支付数量
    #    点击的场合 :  ( 品类ID，( 1, 0, 0 ) )
    #     下单的场合 : ( 品类ID，( 0, 1, 0 ) )
    #     支付的场合 : ( 品类ID，( 0, 0, 1 ) )

    def f_flatmap(line):
        """

        :param line:  文本的一行
        :return:
        """
        arr = line.split('_')
        category = arr[6]
        order = arr[8]
        pay = arr[10]

        if category != '-1':
            return [(category, (1, 0, 0))]

        elif order != 'null':

            ids = order.split(',')

            tuples = []

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    tuples.append((id, (0, 1, 0)))

            return tuples

        elif pay != 'null':

            ids = pay.split(',')

            tuples = []

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    tuples.append((id, (0, 0, 1)))

            return tuples

        else:
            return []

    category_order_pay_rdd = facts_table.flatMap(f_flatmap)

    print(category_order_pay_rdd.take(10))

    # [('16', (1, 0, 0)), ('19', (1, 0, 0)), ('12', (1, 0, 0)), ('15', (0, 0, 1)), ('1', (0, 0, 1)), ('20', (0, 0, 1)),
    #  ('6', (0, 0, 1)), ('4', (0, 0, 1)), ('15', (0, 1, 0)), ('13', (0, 1, 0))]

    def f_reducebykey(value1, value2):

        category = value1[0] + value2[0]
        order = value1[1] + value2[1]
        pay = value1[2] + value2[2]

        return (category, order, pay)

    count_category_order_pay_rdd = category_order_pay_rdd.reduceByKey(f_reducebykey)

    # print(count_category_order_pay_rdd.take(10))

    # 2. 将品类进行排序，并且取前10名
    # 点击数量排序，下单数量排序，支付数量排序
    # 元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    # ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )

    result = count_category_order_pay_rdd.sortBy(lambda x: x[1], ascending=False)

    print(result.take(10))
    # [('15', (6120, 1672, 1259)), ('2', (6119, 1767, 1196)), ('20', (6098, 1776, 1244)), ('12', (6095, 1740, 1218)), ('11', (6093, 1781, 1202)), ('17', (6079, 1752, 1231)), ('7', (6074, 1796, 1252)), ('9', (6045, 1736, 1230)), ('19', (6044, 1722, 1158)), ('13', (6036, 1781, 1161))]


def run_v3(file_dir):
    """
    使用累加器 代替 reduceByKey , 这样完全不用 shuffle

    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("HotCategoryTop10Analysis")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)  # 120s

    sc = SparkContext(conf=conf)

    sliceNum = 8

    facts_table = sc.textFile(file_dir, sliceNum)

    hot_category_acc = sc.accumulator(dict(), HotCategoryAccumulator())

    def f_foreach(line):
        """

        :param line:  文本的一行
        :return:
        """
        arr = line.split('_')
        category = arr[6]
        order = arr[8]
        pay = arr[10]

        if category != '-1':

            hot_category_acc.add({category: 0})

        elif order != 'null':

            ids = order.split(',')

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    hot_category_acc.add({id: 1})

        elif pay != 'null':

            ids = pay.split(',')

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    hot_category_acc.add({id: 2})

    facts_table.foreach(f_foreach)

    # print(hot_category_acc.value)

    count_category_order_pay = hot_category_acc.value.items()

    result = sorted(count_category_order_pay, key=lambda x: x[1], reverse=True)

    print(result[:10])

# [('15', [6120, 1672, 1259]), ('2', [6119, 1767, 1196]), ('20', [6098, 1776, 1244]), ('12', [6095, 1740, 1218]),
# ('11', [6093, 1781, 1202]), ('17', [6079, 1752, 1231]), ('7', [6074, 1796, 1252]), ('9', [6045, 1736, 1230]),
# ('19', [6044, 1722, 1158]), ('13', [6036, 1781, 1161])]

# TODO: 报错 java.io.EOFException at java.io.DataInputStream (socket),  原因未知
# class HotCategory:
#
#     def __init__(self, cid):
#
#         self.cid = cid
#         self.clickCnt = 0
#         self.orderCnt = 0
#         self.payCnt = 0


class HotCategoryAccumulator(AccumulatorParam):
    """
    自定义累加器

    继承 AccumulatorParam

    引用
    https://towardsdatascience.com/custom-pyspark-accumulators-310f63ca3c8c

    """

    def zero(self, init_value):
        return init_value

    def addInPlace(self, dict1, dict2):
        # dict1: 上一个 dict

        first_k_v = next(iter(dict2.items()))
        first_v = first_k_v[1]

        # 通过 value 的类型, 判断现在在哪个阶段(map , merge)
        if isinstance(first_v, int):
            # 在 executor 端进行 map
            # dict2 = {'16': 1}  # (品类ID , 行为类型 id)

            for key, value in dict2.items():
                self.map_func(dict1, key, value)

        elif isinstance(first_v, list):
            # 在 driver 端进行 merge
            # dict2 = {'16': [1, 0, 0], '19': [1, 0, 0], '12': [1, 0, 0]}

            self.merge(dict1, dict2)

        return dict1

    def merge(self, dict1, dict2):

        for key, value in dict2.items():

            if key in dict1:

                for i in range(3):
                    dict1[key][i] += dict2[key][i]
            else:
                dict1[key] = dict2[key]

    def map_func(self, prev_dict, key, value):

        category_id = key
        action_type = value

        if category_id not in prev_dict:
            hot_category = [0, 0, 0]

        else:
            hot_category = prev_dict[category_id]

        if action_type == 0:  # 'click'

            hot_category[0] += 1

        elif action_type == 1:  # 'order'

            hot_category[1] += 1

        elif action_type == 2:  # 'pay'

            hot_category[2] += 1

        prev_dict[category_id] = hot_category


if __name__ == '__main__':
    data_dir = '../data/'

    file_dir = os.path.join(data_dir, 'user_visit_action.txt')

    run_v3(file_dir)
