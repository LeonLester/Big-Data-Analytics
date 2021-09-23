import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import year, month, dayofmonth, hour, row_number, col,  window, sum, collect_list, concat_ws, substring_index
from pyspark.sql.window import Window

from sparkmeasure import StageMetrics


import time


spark = SparkSession.builder.getOrCreate()
print(spark,type(spark))

movies = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("../csv/movie.csv"))
tags = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("../csv/tag.csv"))
ratings = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("../csv/rating.csv"))

movies = spark.read.csv("../csv/movie.csv",header=True)
ratings = spark.read.csv("../csv/rating.csv",header=True)



def stagemetricstimer(func):
    def wrapper():
        stagemetrics = StageMetrics(spark)
        stagemetrics.begin()
        func()
        stagemetrics.end()
        stagemetrics.print_report()

    return wrapper()


def one():
    Jumanji = movies.filter(movies.title=="Jumanji (1995)").collect()[0]["movieId"] #select(movies.movieId.alias("Id"))
    print(ratings.filter(ratings.movieId==Jumanji).count())

def two():
    boringTags = tags.filter(tags["tag"].contains("boring")).select(tags["tag"],tags["movieId"])
    boringMovies = boringTags.join(movies, boringTags["movieId"]==movies["movieId"], "inner").select(movies["title"])
    boringMovies.distinct().orderBy('title').show(5)
    print(boringMovies.count())
    
def three():
    bollywoodTags = tags.filter(tags["tag"]== "bollywood").select(tags["userId"],tags["movieId"])
    threeRatings = ratings.filter(ratings["rating"]>3).select(ratings["userId"],ratings["movieId"])
    joined = threeRatings.join(bollywoodTags,(threeRatings["movieId"]==bollywoodTags["movieId"])&(threeRatings["userId"]==bollywoodTags["userId"]) , "inner").select(threeRatings['userId']).distinct().orderBy(threeRatings["userId"]).show(5)#select(threeRatings["userId"])
    print(joined.count())
    
def four():    
    
    avgRatings = ratings.groupBy(col("movieId"), year("timestamp").alias("timestamp")).agg(F.avg("rating").alias("rating"))

    topMovieIds = avgRatings.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 10)

    movies.join(topMovieIds,(movies["movieId"]==topMovieIds["movieId"]),"inner").select("*").show()
    
def five():  
    
    movies2015 = tags.filter((tags.timestamp >= "2015-01-01") & (tags.timestamp <= "2015-12-31"))
    movies2015.join(movies,"movieId").groupBy("title").agg(concat_ws(",",collect_list("tag"))).orderBy("title").show(truncate=100) #select("title","tag").show()
    
    
def six(): 

    ratingCount = ratings.groupBy("movieId").count().select("movieId", F.col('count').alias('rating count'))
    ratingCount.select("*").orderBy(ratingCount["rating count"].desc()).show(5)
    
def seven():
    temp = ratings.groupBy(year("timestamp"),"userId").count()
    windowDept = Window.partitionBy("year(timestamp)").orderBy(col("count").desc())
    temp.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 10).orderBy("year(timestamp)","row").filter(temp["year(timestamp)"]=="1995").show()
    
    
def eight():
    splitMovieGenre = movies.select(movies["movieId"],movies["title"],substring_index(movies["genres"], "|", 1).alias("genre"))
    movieRatingsCount = ratings.groupBy("movieId").count().select("movieId",F.col('count').alias('ratingCount'))
    movieRatings = movieRatingsCount.join(splitMovieGenre,(movieRatingsCount["movieId"]==splitMovieGenre["movieId"]),"inner")\
    .select(movieRatingsCount["movieId"],movieRatingsCount["ratingCount"],splitMovieGenre["title"],splitMovieGenre["genre"])

    groupByGenre = movieRatings.groupBy("genre").agg(F.max("ratingCount").alias("ratingCount"), F.first("title").alias("title"))\
    .orderBy("genre").show(5,False)
    
def nine():
    temp = ratings.groupBy(year("timestamp"),month("timestamp"), dayofmonth("timestamp"),hour("timestamp"),"movieId").count().orderBy("count",ascending=False)
    temp = temp.filter(temp["count"] > 1).distinct()
    temp = temp.select(sum(temp["count"]))
    temp.show()
    
def ten():
    
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
    
    
    
if __name__ == "__main__":
    print("--------------------------------------------")
    funcdict = {"1":"one",
                "2":"two",
                "3":"three",
                "4":"four",
                "5":"five",
                "6":"six",
                "7":"seven",
                "8":"eight",
                "9":"nine",
                "10":"ten"}
    print(funcdict)
    chosenFunction = str(sys.argv[1])
    
    stagemetricstimer(funcdict[chosenFunction])
    
#stagemetrics = StageMetrics(spark)

