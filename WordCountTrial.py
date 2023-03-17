from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
 spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("WordCount") \
    .getOrCreate()

lines = spark.read.text('C:\\Users\\deogracias.plaud\\Documents\\deckofcards.txt')

wordCounts = lines \
    .select(explode(split(lines.value, "\s+"))
            .alias("word")) \
    .groupBy("word") \
    .count()

print("\n Word Count : ", wordCounts.show(20, 5))

spark.stop()
