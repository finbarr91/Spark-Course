from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

friends_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(r"C:\Users\chukw\PycharmProjects\Spark-Course\fakefriends-header.csv")


print("Here is our inferred schema:")
friends_df.printSchema()

print("Let's display the age and friends column:")
friends = friends_df.select("age","friends")
friends.show()

print("Let's display the average of friends ")
friends.groupBy('age').avg('friends').show()

#Sorted
friends.groupBy('age').avg('friends').sort('age').show()

# Formatted more nicely
friends.groupBy('age').agg(func.round(func.avg('friends'),2).alias('friends_avg')).sort('age').show()

spark.stop()