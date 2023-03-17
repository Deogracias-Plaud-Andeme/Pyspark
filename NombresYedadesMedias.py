from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import avg

spark = (SparkSession
 .builder
 .appName("EdadGente")
 .getOrCreate())

data_df = spark.createDataFrame([("Marcos", 36), ("Dennis", 79), ("Maria", 19),
 ("Oleg", 65), ("Ann", 50)], ["name", "age"])

avg_df = data_df.groupBy("name").agg(avg("age"))

avg_df.show()
