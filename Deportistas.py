from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql import Row

rows = [Row("Lebron James", "L.A. LAKERS"), Row("Lionel Messi", "PSG"), Row("Cristiano Ronaldo", "Al Nassir")]
authors_df = spark.createDataFrame(rows, ["Deportista", "Club"])
authors_df.show()

