import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, max
from pyspark.sql.window import Window


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sourceTable", help="Source Table", required=True)
    parser.add_argument("-m", "--master", help="yarn or local", required=True)
    parser.add_argument("-a", "--appName", help="Application Name", required=False)

    args = parser.parse_args()
    process(args.master, args.source_table)


def configureSpark(master, appName):
    return (
        SparkSession.builder
            .master(master)
            .appName(appName)
            .enableHiveSupport()
            .getOrCreate()
    )


def process(master, source_table):
    spark = configureSpark(master, "CareerFoundry")
    time_period = 5
    # Temp Data Directory for Local Test
    output_directory = "/Users/k0s0972/Documents/Projects/CareerFound/kuldeepsingh20-data-engineering-test/CareerFoundry/Output/"
    try:

        recipe_df = spark.table("bitcoin_data_table")

        # generating row number on time_period_start to perofrm rolling window aggregation
        bitcoin_df = recipe_df.withColumn("rank", F.row_number().over(Window.orderBy("time_period_start")))

        # Rolling window with timeperiod(5 in current setting)
        windowSpec = Window.orderBy("rank").rangeBetween(int("-{}".format(time_period)), 0)

        # if row number is less than time_period, setting averages to null else rolling aggregation
        # Creating columns simple_moving_avg_5_period, stddev_5_period
        transformed_df = bitcoin_df.withColumn("simple_moving_avg_5_period", when(col("rank") <= time_period - 1, None) \
                                               .otherwise(F.mean(col('price_close')).over(windowSpec))). \
            withColumn("stddev_5_period", when(col("rank") <= time_period - 1, None) \
                       .otherwise(F.stddev(col('price_close')).over(windowSpec))).withColumn("date", F.to_date(
            "time_period_start")) \
            .orderBy(F.desc("date"))

        # selecting useful columns
        volality_data_df = transformed_df.select("time_period_start", "time_period_end", "price_open",
                                                 "price_high", "price_low", "price_close", "volume_traded",
                                                 "trades_count", "simple_moving_avg_5_period", "stddev_5_period",
                                                 "date")
        volality_data_df.printSchema()

        spark.sql("drop table IF EXISTS bitcoin_volatility_table")
        # Creating Table with standard Deviation and simple moving average columns
        spark.sql(
            "CREATE TABLE IF NOT EXISTS bitcoin_volatility_table ( time_period_start timestamp, time_period_end "
            "timestamp, price_open double, price_high double, price_low double, price_close double, volume_traded "
            "double, trades_count integer, simple_moving_avg_5_period double, stddev_5_period double, date date )")

        # Just for Evaluation Purpose, Need to Comment bellow line when using in production
        volality_data_df.show(5, False)

        # writing data in overwrite mode
        volality_data_df.write.mode("overwrite").insertInto("bitcoin_volatility_table")
        # Just for Output Reference purpose
        volality_data_df.coalesce(1).write.mode("overwrite").csv("{}/VolatilityData/".format(output_directory))

        # Part 2 : Calculation Day wise Data from source Data
        # this will be useful in other calculations to predict future movement in the price.

        # Creating Dense Rank Columns date wise, to give rank 1 to first record(to extract Day Open), simmilarly
        # order_desc to get day close
        ranked_transformed_df = transformed_df.withColumn("order_asc", F.dense_rank().over(
            Window.partitionBy("date").orderBy("time_period_start"))).withColumn("order_desc", F.dense_rank().over(
            Window.partitionBy("date").orderBy(F.desc("time_period_start"))))

        # Filtering Day openPrice and Close Price Data
        filtered_ranked_transformed_df = ranked_transformed_df.withColumn("open_price", ranked_transformed_df.where(
            ranked_transformed_df.order_desc == 1)["price_open"]).withColumn("close_price", ranked_transformed_df.where(
            ranked_transformed_df.order_desc == 1)["price_close"])

        # Selecting useful columns
        filtered_df = filtered_ranked_transformed_df.select("date", "open_price", "close_price")
        # Group by to get min and max price day wise
        grouped_df = filtered_ranked_transformed_df.groupBy("date").agg(min("price_low").alias("min_price"),
                                                                        max("price_high").alias("high_price"))
        # joining to get all columns, min_price, high_price, open_price, close_price
        day_data_df = grouped_df.join(filtered_df, "date", "inner")
        # Just for Evaluation Purpose, Need to Comment bellow line when using in production
        day_data_df.show(5, False)
        day_data_df.printSchema()

        spark.sql("drop table IF EXISTS bitcoin_day_data_table")
        spark.sql(
            "CREATE TABLE IF NOT EXISTS bitcoin_day_data_table ( date date, min_price Double, high_price Double, "
            "open_price Double, close_price Double )")

        # Full refresh Data
        day_data_df.write.mode("overwrite").insertInto("bitcoin_day_data_table")
        # Just for Output Reference purpose
        day_data_df.coalesce(1).write.mode("overwrite").csv("{}/DayData/".format(output_directory))

    except Exception as e:
        raise Exception("Error in Transformation")


if __name__ == '__main__':
    main()
