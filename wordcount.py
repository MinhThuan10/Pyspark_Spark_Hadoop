from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, desc
import os

spark = SparkSession.builder \
    .appName("PythonWordCount") \
    .master("spark://192.168.174.231:7077") \
    .getOrCreate()

input_dir = "hdfs://MinhThuan:9820/user/input/wordcount/*"

text_files = spark.read.text(input_dir)

words = text_files.select(explode(split(text_files.value, r"\s+")).alias("word"))

word_count = words.groupBy("word").count()

word_count_sorted = word_count.orderBy(desc("count"))

word_count_sorted.show()

output_path = "hdfs://MinhThuan:9820/user/output/wordcount"
word_count_sorted.write.mode("overwrite").csv(output_path)

spark.stop()
