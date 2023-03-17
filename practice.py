from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *

data = [(1,'Deo'),(2,'Plaud')]
schema = StructType([StructField(name='id',dataType=IntegerType()),
                     StructField(name='name',dataType=StringType())])

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()