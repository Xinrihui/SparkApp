
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import os

def wordcount1(file_dir):

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

    result = file.flatMap(f_flatmap).map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)

    result = result.sortBy(lambda x: x[1], ascending=False)

    print(result.take(10))


if __name__ == '__main__':

    data_dir = '../data/'

    file_dir = os.path.join(data_dir, 'apple.txt')

    wordcount1(file_dir)

