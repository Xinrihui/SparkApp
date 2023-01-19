
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# API doc
# https://spark.apache.org/docs/latest/api/python/reference/index.html

from pyspark.sql.types import IntegerType, StructType, StructField, StringType

from pyspark.sql.types import DataType, TimestampType, DoubleType, FloatType

import os

# input_path = 'data/input/electric-chargepoints-2017.csv'
# output_path = 'data/output/chargepoints-2017-analysis'

class ChargePointsETLJob:



    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("ElectricChargePointsETLJob")
                              .getOrCreate())

    def extract(self):

        spark = self.spark_session

        input_path = os.path.join('../data/input/', 'electric-chargepoints-2017.csv')
        # input_path = 'data/input/electric-chargepoints-2017.csv'

        schema1 = StructType() \
            .add(StructField("charging_event", IntegerType(), True)) \
            .add(StructField("chargepoint_id", StringType(), True)) \
            .add(StructField("start_date", StringType(), True)) \
            .add(StructField("start_time", StringType(), True)) \
            .add(StructField("end_date", StringType(), True)) \
            .add(StructField("end_time", StringType(), True)) \
            .add(StructField("energy", StringType(), True)) \
            .add(StructField("plugin_duration", DoubleType(), True))

        df = spark.read.csv(header=False, schema=schema1, sep=',',
                                 path=input_path)

        return df

    def transform(self, df):

        spark = self.spark_session

        df.createOrReplaceTempView("electric_chargepoints")

        df2 = spark.sql(
            '''
            select 
            chargepoint_id,
            round(max(plugin_duration),2) as max_duration,
            round(avg(plugin_duration),2) as avg_duration
            
            from 
            electric_chargepoints 
            group by chargepoint_id
            '''
        )

        df2.show()

        return df2


    def load(self, df):

        output_path = '../data/output/chargepoints-2017-analysis'
        # output_path = 'data/output/chargepoints-2017-analysis'

        df.write.parquet(output_path)


    def run(self):
        self.load(self.transform(self.extract()))

if __name__ == '__main__':

    obj = ChargePointsETLJob()

    # obj.extract().show()

    # obj.transform(obj.extract()).show()

    obj.run()