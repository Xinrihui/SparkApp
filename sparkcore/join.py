#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os


import re

from pyspark.sql import SparkSession


class Join:
    """
    by XRH
    date: 2020-06-16

    利用 spark 的基础算子 实现 常见的 join 算法

    功能：
    1. 朴素的 MapReduce 的 join
    2. 基于 广播变量的 hash join
    3.

    """

    def common_join(self ,table_a_dir ,table_b_dir ,table_dir):
        """
         利用基本算子 实现 MapReduce 的 join
        :param table_a:
        :param table_b:
        :return:
        """
        spark = SparkSession.builder.appName("common_Join").getOrCreate()
        sc = spark.sparkContext

        sliceNum = 2
        table_a = sc.textFile(table_a_dir, sliceNum) # 2个分区
        table_b = sc.textFile(table_b_dir, sliceNum) # 2个分区

        table_a = table_a.map(lambda line: line.split(','))
        table_b = table_b.map(lambda line: line.split(','))

        table_a = table_a.map(lambda line: (line[0], line[1:]))  # 只能有 两个元素 ，第1个为 Key; 否则后面的 groupByKey() 报错
        table_b = table_b.map(lambda line: (line[0], line[1:]))

        table = table_a.union(table_b)  # 合并后 分区的数目 也是 两个 RDD 的分区的和

        # table.glom().collect() # 输出 各个分区 的元素 列表

        # [[('1', ['a', '27']), ('2', ['b', '24']), ('3', ['c', '23'])],
        #  [('4', ['d', '21']), ('5', ['e', '22']), ('6', ['f', '20'])],
        #  [('1', ['male']), ('2', ['female'])],
        #  [('4', ['female']), ('5', ['male'])]]
        # 可以看出一共有4个分区

        # 重新划分为2个分区, 默认采用 hash 分区, 因此 key 相同的会被 shuffle 到1个分区中
        table = table.partitionBy(2)

        # 1.此处原理与 MapReduce 不同, MapReduce 肯定会做shuffle
        # 一般 1个hdfs 的block对应 1个map task, 在1个map task中:
        # (1) 在环形缓冲区, 数据按照 分区+key 进行快速排序了
        # (2) 环形缓冲区溢出到磁盘, 对每一个分区对应的多个溢出文件进行归并排序, 最后生成 分区文件, 一个分区对应一个文件

        # 1个分区对应 1个reduce task, 在1个reduce task中:
        # (1) 去拉取 map task 在磁盘上的, 我对应要处理的分区文件, 然后进行归并排序
        # (2) 从归并排序后的文件中, 按顺序提取出 (key, key 对应的 value-list ) 输入给reduce 函数,
        #     如果是两张表join, 则此步骤相当于完成了按照key的join 操作

        # 2. 可以看出 spark 相较于 MapReduce ,操作更加灵活, 在spark 中shuffle 是可选的


        # table.glom().collect()

        # [[('1', ['a', '27']), ('4', ['d', '21']), ('1', ['male']), ('4', ['female'])],
        #  [('2', ['b', '24']),
        #   ('3', ['c', '23']),
        #   ('5', ['e', '22']),
        #   ('6', ['f', '20']),
        #   ('2', ['female']),
        #   ('5', ['male'])]]
        # 可以看出一共有2个分区, 并且相同的 key 在同一分区

        def process_oneslice(one_slice, col_num):
            """
            对一个分区的处理

            :param one_slice:
            :param col_num:
            :return:
            """

            res = []

            hash_table = {}

            for line in one_slice:

                key = line[0]
                value = line[1]

                if key not in hash_table:
                    hash_table[key] = value

                else:
                    hash_table[key] = hash_table[key] + value

            for key, value in hash_table.items():

                if len(value) == col_num:  # 这一行的 col 个数 匹配 说明 关联成功

                    res.append([key] + value)

            return res

        col_num = 3  # 最终表 除了 Key 之外 应该有 3 个列（字段）
        table = table.mapPartitions(lambda one_slice: process_oneslice(one_slice, col_num))

        # table.glom().collect()

        table_one_slice = table.map(lambda line: ",".join(line)).coalesce(1, shuffle=True)  # 输出为 一个切片

        table_one_slice.saveAsTextFile(table_dir)


    def hash_join(self ,table_a_dir ,table_b_dir ,table_dir):
        """
        利用 基本 算子 实现 hash join
        :return:
        """

        spark = SparkSession.builder.appName("hash_join").getOrCreate()
        sc = spark.sparkContext

        sliceNum = 2
        table_a = sc.textFile(table_a_dir, sliceNum)
        table_b = sc.textFile(table_b_dir, sliceNum)

        table_a = table_a.map(lambda line: line.split(','))  # 大表
        table_b = table_b.map(lambda line: line.split(','))  # 小表

        table_a = table_a.map(lambda line: (line[0], line[1:]))  # 只能有 两个元素 ，第1个为 Key; 否则后面的 groupByKey() 报错
        table_b = table_b.map(lambda line: (line[0], line[1:]))

        table_b = table_b.collect()  # [('1', ['male']), ('2', ['female']), ('4', ['female']), ('5', ['male'])]

        hash_table_b = {} # 把小表 做成 hash 表

        for line in table_b:
            hash_table_b[line[0]] = line[1][0]

        # 把小表 作为 广播变量 分发到各个 计算节点上
        broadcast_table_b = sc.broadcast(hash_table_b)  # SPARK-5063: RDD 不能被广播

        def process_oneslice(big_table_slice):

            res = []

            for line in big_table_slice:

                key = line[0]

                values = line[1]

                if key in broadcast_table_b:
                    res.append([key] + [hash_table_b[key]] + values)

            return res

        table = table_a.mapPartitions(lambda big_table_slice: process_oneslice(big_table_slice))

        # table.collect()

        table_one_slice = table.map(lambda line: ",".join(line)).coalesce(1, shuffle=True)  # 输出为 一个切片

        table_one_slice.saveAsTextFile(table_dir)



    def shuffle_Hash_join(self):
        """
        实现 一个 基于分区 的 Join
        :return:
        """
        spark = SparkSession.builder.appName("backet_map_join").getOrCreate()
        sc = spark.sparkContext

        # TODO: 如何 同时 操作 两个分区 中的数据， eg. 把一个 分区中的 数据  放入 内存中 做成 hash 表，与另一个分区 关联

if __name__ == '__main__':


    #---------------  join 函数 测试 -------------#
    data_dir = '../data/'

    table_a_dir = os.path.join(data_dir, 'table_A')

    table_b_dir = os.path.join(data_dir, 'table_B')

    table_dir = os.path.join(data_dir, 'table')

    sol2=Join()

    sol2.common_join(table_a_dir, table_b_dir, table_dir)

    # sol2.hash_join(table_a_dir, table_b_dir, table_dir)