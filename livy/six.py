from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("6").getOrCreate()


import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, year

ratings = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("/home/administrator/Downloads/movielens/rating.csv"))


ratingCount = ratings.groupBy("movieId").count().select("movieId", F.col('count').alias('rating count'))

ratingCount.select("*").orderBy(ratingCount["rating count"].desc()).show(5)

