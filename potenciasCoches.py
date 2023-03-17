from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window

schema =StructType().add(field='name',data_type=StringType())\
    .add(field='company',data_type=StringType())\
    .add(field='power',data_type=IntegerType())

df = spark.read.csv(path='C:\Big Data\ArchivosPractica\potenciasCoches.csv', schema=schema, header=True)

df.sort('company').show()
window = Window.partitionBy('company').orderBy('power')

df.withColumn('Row Number', row_number().over(window))\
    .withColumn('Rank', rank().over(window))\
    .withColumn('Dense Rank', dense_rank().over(window)).show()

