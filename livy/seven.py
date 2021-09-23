from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, year
spark = SparkSession.builder.getOrCreate()

ratings = (spark.read
     .format("csv")
     .option('header', 'true')
     .option("delimiter", ",")
     .option("inferSchema", "true")
     .load("/home/administrator/Downloads/movielens/rating.csv"))

temp = ratings.groupBy(year("timestamp"),"userId").count()

windowDept = Window.partitionBy("year(timestamp)").orderBy(col("count").desc())

temp.withColumn("row",row_number().over(windowDept)).filter(col("row") <= 10).orderBy("year(timestamp)","row").filter(temp["year(timestamp)"]=="1995").show()