from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the input text file
text_file = spark.read.text("gs://dibimbing_bucket-1/input/rose.txt")
# gs://dibimbing_bucket-1/input/rose.txt
# Perform word count
word_count = text_file.selectExpr("explode(split(value, ' ')) as word").groupBy("word").count()

# Save the result to an output file
word_count.write.csv("gs://dibimbing_bucket-1/output/")
