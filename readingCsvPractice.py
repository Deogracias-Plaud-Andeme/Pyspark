from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(path='C:\Big Data\ArchivosPractica\mnm_dataset.csv',header=True)
#df = spark.read.format('csv').option(key='header',value=True).load('C:\Big Data\ArchivosPractica\mnm_dataset.csv')
df.show()
df.printSchema()
