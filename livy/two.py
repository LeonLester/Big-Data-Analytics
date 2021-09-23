from pyspark.sql import SparkSession



spark = SparkSession \
    .builder\
    .getOrCreate()
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



boringTags = tags.filter(tags["tag"].contains("boring")).select(tags["tag"],tags["movieId"])

boringMovies = boringTags.join(movies, boringTags["movieId"]==movies["movieId"], "inner").select(movies["title"])

boringMovies.distinct().orderBy('title').show(5)

print(boringMovies.count())
