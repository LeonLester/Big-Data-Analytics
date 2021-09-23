from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("eight")\
    .getOrCreate()

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col, substring_index 

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

splitMovieGenre = movies.select(movies["movieId"],movies["title"],substring_index(movies["genres"], "|", 1).alias("genre"))

movieRatingsCount = ratings.groupBy("movieId").count().select("movieId",F.col('count').alias('ratingCount'))

movieRatings = movieRatingsCount.join(splitMovieGenre,(movieRatingsCount["movieId"]==splitMovieGenre["movieId"]),"inner")\
    .select(movieRatingsCount["movieId"],movieRatingsCount["ratingCount"],splitMovieGenre["title"],splitMovieGenre["genre"])

groupByGenre = movieRatings.groupBy("genre").agg(F.max("ratingCount").alias("ratingCount"), F.first("title").alias("title"))\
    .orderBy("genre").show(5,False)
