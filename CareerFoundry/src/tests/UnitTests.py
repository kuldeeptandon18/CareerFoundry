import unittest
from CareerFoundry.src.main import BitCoinDataTransformation
from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType, DoubleType
from pyspark.sql import SparkSession


class TestMethods(unittest.TestCase):

    def getSchema(self):
        schema = StructType([
            StructField("time_period_start", TimestampType(), True),
            StructField("time_period_end", TimestampType(), True),
            StructField("time_open", TimestampType(), True),
            StructField("time_close", TimestampType(), True),
            StructField("price_open", DoubleType(), True),
            StructField("price_high", DoubleType(), True),
            StructField("price_low", DoubleType(), True),
            StructField("price_close", DoubleType(), True),
            StructField("volume_traded", DoubleType(), True),
            StructField("trades_count", IntegerType(), True)
        ])
        return schema

    def test_volatility(self):
        resource_directory = "resources/"
        self.spark = SparkSession.builder.master("local[*]").enableHiveSupport().getOrCreate()

        recipe_df = self.spark.read.schema(self.getSchema()).option("multiline", "true").json(
            resource_directory + "ohlcv-btc-usd-history-6min-2020.json")
        recipe_df.createOrReplaceTempView("bitcoin_data_table")

        # Calling Process Function

        BitCoinDataTransformation.process("local[*]", "sourceTable")
        result = self.spark.sql("select * from bitcoin_volatility_table where date='2020-08-24'")
        print("result is: {}".format(result.count()))
        self.assertEqual(result.count(), 240)


if __name__ == '__main__':
    unittest.main()
