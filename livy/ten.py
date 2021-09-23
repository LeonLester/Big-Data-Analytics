from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("10").getOrCreate()

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
tags = (spark.read
    .format("csv")
    .option('header', 'true')
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load("/home/administrator/Downloads/movielens/tag.csv"))


singleMovieGenre = movies.select(movies["movieId"],movies["title"],substring_index(movies["genres"], "|", 1).alias("genre"))

funnyMovies = tags.filter(tags["tag"]== "funny").select(tags["movieId"],tags["tag"]).dropDuplicates(['movieId'])

ratingMovies = ratings.filter(ratings["rating"]>=3.5).select(ratings["movieId"],ratings["rating"]).dropDuplicates(['movieId'])

funnyRatingMovies = (funnyMovies
                    .join(ratingMovies, (ratingMovies["movieId"]==funnyMovies["movieId"]), "inner")
                ).select(funnyMovies["movieId"],funnyMovies["tag"],ratingMovies["rating"])

targetMovies = (funnyRatingMovies
                    .join(singleMovieGenre,(singleMovieGenre["movieId"]==funnyRatingMovies["movieId"]), "inner")
                ).select(funnyRatingMovies["*"],singleMovieGenre["genre"])

movieCount = targetMovies.groupBy("genre").count().select("genre",F.col('count').alias('rating_count')).orderBy("genre").show(5,False)
