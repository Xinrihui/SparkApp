
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.accumulators import AccumulatorParam
import os

def wordcount1(file_dir):
    """
    groupBy

    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("wordcount1")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    sliceNum = 8

    file = sc.textFile(file_dir, sliceNum)

    def f_flatmap(line):

        arrs = line.split(' ')

        return [word for word in arrs if word !='']

    rdd1 = file.flatMap(f_flatmap)

    rdd2 = rdd1.groupBy(lambda word: word) # 按照单词分组并聚合

    def f_map(inputs):

        key, val_list = inputs

        # for ele in val_list:
        #     print(ele)

        return key, len(val_list)

    rdd3 = rdd2.map(f_map)

    result = rdd3.sortBy(lambda x: x[1], ascending=False) # 按照出现次数逆序

    print(result.take(10)) # 出现次数最高的10个单词

    sc.stop()

def wordcount2(file_dir):
    """
    groupByKey


    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("wordcount2")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    sliceNum = 8

    file = sc.textFile(file_dir, sliceNum)

    def f_flatmap(line):

        arrs = line.split(' ')

        return [word for word in arrs if word !='']

    rdd1 = file.flatMap(f_flatmap).map(lambda word: (word, 1))

    print(rdd1.collect())

    rdd2 = rdd1.groupByKey() # 按照单词分组并聚合

    print(rdd2.collect())

    def f_map(inputs):

        key, val_list = inputs

        cnt = 0

        for ele in val_list:
            cnt += ele

        return key, cnt

    rdd3 = rdd2.map(f_map)

    result = rdd3.sortBy(lambda x: x[1], ascending=False)

    print(result.take(10))
    # [('and', 23), ('of', 8), ('data', 7), ('in', 7), ('the', 6), ('to', 6), ('a', 5), ('Apple,', 3), ('that', 3), ('experience', 3)]
    sc.stop()


def wordcount3(file_dir):
    """
    reduceByKey

    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("wordcount3")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    sliceNum = 8

    file = sc.textFile(file_dir, sliceNum)

    def f_flatmap(line):

        arrs = line.split(' ')

        return [word for word in arrs if word !='']

    result = file.flatMap(f_flatmap).map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)

    result = result.sortBy(lambda x: x[1], ascending=False)

    print(result.take(10))

    sc.stop()

def wordcount4(file_dir):
    """
    使用累加器实现, 效率最高


    :param file_dir:
    :return:
    """

    conf = SparkConf()
    conf.setMaster("local").setAppName("wordcount3")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)

    sc = SparkContext(conf=conf)

    sliceNum = 8

    file = sc.textFile(file_dir, sliceNum)

    def f_flatmap(line):

        arrs = line.split(' ')

        return [word for word in arrs if word !='']

    rdd1 = file.flatMap(f_flatmap)

    word_acc = sc.accumulator(dict(), WordCountAccumulator()) # 定义累加器

    def f_foreach(inputs):

        word = inputs
        word_acc.add({word: 1})

    rdd1.foreach(f_foreach)

    result = word_acc.value.items()

    result = sorted(result, key=lambda x: x[1], reverse=True)

    print(result[:10])

    sc.stop()

class WordCountAccumulator(AccumulatorParam):
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
        # dict2: 当前 dict

        first_k_v = next(iter(dict2.items()))
        first_v = first_k_v[1]

        # 通过 value 的类型, 判断现在在哪个阶段(map , merge)
        if isinstance(first_v, int):
            # 在 executor 端进行 map
            #  dict2 = {'apple': 1}  # (单词, 出现次数)

            for key, value in dict2.items():
                self.map_func(dict1, key, value)

        elif isinstance(first_v, list):
            # 在 driver 端进行 merge
            # dict2 = {'apple': 2, 'the': 10}

            self.merge(dict1, dict2)

        return dict1

    def merge(self, dict1, dict2):

        for key, value in dict2.items():

            if key in dict1:
                dict1[key] += dict2[key]

            else:
                dict1[key] = dict2[key]

    def map_func(self, prev_dict, key, value):

        if key in prev_dict:
            prev_dict[key] += value

        else:
            prev_dict[key] = value



if __name__ == '__main__':

    data_dir = '../data/'

    file_dir = os.path.join(data_dir, 'apple.txt')

    wordcount4(file_dir)

