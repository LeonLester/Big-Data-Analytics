from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import collect_list, concat_ws
spark = SparkSession.builder.getOrCreate()

movies = (spark.read
    .format("csv")
    .option('header', 'true')
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load("/home/administrator/Downloads/movielens/movie.csv"))

tags = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("/home/administrator/Downloads/movielens/tag.csv"))

movies2015 = tags.filter((tags.timestamp >= "2015-01-01") & (tags.timestamp <= "2015-12-31"))

movies2015.join(movies,"movieId").groupBy("title").agg(concat_ws(",",collect_list("tag"))).orderBy("title").show(truncate=100)
