from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, explode, split, col, concat_ws, collect_set, substring_index

spark = SparkSession.builder \
    .appName("Comprehension_Spark") \
    .master("spark://192.168.174.231:7077") \
    .getOrCreate()

input_dir = "hdfs://MinhThuan:9820/user/input/wordcount/*"

words = spark.read.text(input_dir).select(input_file_name().alias("file"), "value")

word_file_pairs = words.select(input_file_name().alias("file"), explode(split(col("value"), " ")).alias("word"))

grouped_words = word_file_pairs.groupBy("word").agg(concat_ws(", ", collect_set(substring_index(col("file"), "/", -1))).alias("files"))

sorted_grouped_words = grouped_words.orderBy("word")

sorted_grouped_words.show(truncate=False)

output_dir = "hdfs://MinhThuan:9820/user/output/comprehension"
sorted_grouped_words.write.mode("overwrite").csv(output_dir)

spark.stop()
