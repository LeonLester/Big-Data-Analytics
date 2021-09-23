from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, dayofmonth, hour, row_number, window, year, sum
from pyspark.sql.window import Window


spark = SparkSession.builder.getOrCreate()


movies = spark.read.csv("/home/administrator/Downloads/movielens/movie.csv", header=True, inferSchema=True)

ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv", header=True, inferSchema=True)


Jumanji = movies.filter(movies.title=="Jumanji (1995)").collect()[0]["movieId"] #select(movies.movieId.alias("Id"))

print(ratings.filter(ratings.movieId==Jumanji).count())

