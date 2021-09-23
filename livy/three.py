from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
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
ratings = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("/home/administrator/Downloads/movielens/rating.csv"))



bollywoodTags = tags.filter(tags["tag"]== "bollywood").select(tags["userId"],tags["movieId"])

threeRatings = ratings.filter(ratings["rating"]>3).select(ratings["userId"],ratings["movieId"])

joined = threeRatings.join(bollywoodTags,(threeRatings["movieId"]==bollywoodTags["movieId"])&(threeRatings["userId"]==bollywoodTags["userId"]) , "inner").select(threeRatings['userId']).distinct().orderBy(threeRatings["userId"]).show(5)

print(joined)
