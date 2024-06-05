from pyspark.sql import SparkSession
from pyspark.sql.functions import split


spark = SparkSession.builder \
    .appName("FriendCount_Spark") \
    .master("spark://192.168.174.231:7077") \
    .getOrCreate()


input_file_path = "hdfs://MinhThuan:9820/user/input/friendcount/friends"


text_file = spark.read.text(input_file_path)

friend_pairs = text_file.select(split(text_file.value, ",").alias("friend_pair")) \
                         .selectExpr("friend_pair[0] as friend")
friend_pairs.show()

friend_count = friend_pairs.groupBy("friend").count()


friend_count.show()

output_file_path = "hdfs://MinhThuan:9820/user/output/friendcount"
friend_count.write.mode("overwrite").csv(output_file_path)


spark.stop()
