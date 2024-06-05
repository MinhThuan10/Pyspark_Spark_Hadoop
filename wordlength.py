from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, length, col, split

spark = SparkSession.builder \
    .appName("WordLength_Spark") \
    .master("spark://192.168.174.231:7077") \
    .getOrCreate()

input_dir = "hdfs://MinhThuan:9820/user/input/wordlength/*"

words = spark.read.text(input_dir).select(explode(split(col("value"), " ")).alias("word"))

word_categories = words.select(col("word"), length(col("word")).alias("length")) \
                       .selectExpr("word", "CASE WHEN length = 1 THEN 'tiny' " +
                                   "WHEN length >= 2 AND length <= 4 THEN 'small' " +
                                   "WHEN length >= 5 AND length <= 9 THEN 'medium' " +
                                   "ELSE 'big' END AS category")

word_count = word_categories.groupBy("category").count()

word_count.show()

output_path = "hdfs://MinhThuan:9820/user/output/wordlength"
word_count.write.mode("overwrite").csv(output_path)

spark.stop()
