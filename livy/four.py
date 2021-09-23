from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("4").getOrCreate()

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, row_number, year

windowDept = Window.partitionBy("timestamp").orderBy(col("rating").desc())

ratings = (spark.read
    .format("csv")
    .option('header', 'true')
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load("/home/administrator/Downloads/movielens/rating.csv"))
movies = (spark.read
    .format("csv")
    .option('header', 'true')
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load("/home/administrator/Downloads/movielens/movie.csv"))



avgRatings = ratings.groupBy(col("movieId"), year("timestamp").alias("timestamp")).agg(F.avg("rating").alias("rating"))

topMovieIds = avgRatings.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 10)

movies.join(topMovieIds,(movies["movieId"]==topMovieIds["movieId"]),"inner").select("*").show()

