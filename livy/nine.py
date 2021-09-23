from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import year, month, dayofmonth, hour

spark = SparkSession.builder.getOrCreate()

ratings = spark.read.csv("/home/administrator/Downloads/movielens/rating.csv",header=True)

temp = ratings.groupBy(year("timestamp"),month("timestamp"), dayofmonth("timestamp"),hour("timestamp"),"movieId").count().orderBy("count",ascending=False)

temp = temp.filter(temp["count"] > 1).distinct()

temp = temp.select(sum(temp["count"]))

temp.show()
