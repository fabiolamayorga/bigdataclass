from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, StringType)

spark = SparkSession.builder.appName("Read Data of Homework").getOrCreate()


## Lee estudiante.csv
csv_schema = StructType([StructField('numeroCarnet', IntegerType()),
                         StructField('nombreCompleto', StringType()),
                         StructField('carrera', StringType()),
                         ])

dataframe = spark.read.csv("estudiante.csv",
                           schema=csv_schema,
                           header=False)

dataframe.show()


#Lee curso.csv
csv_schema = StructType([StructField('codigoCurso', IntegerType()),
                         StructField('creditos', StringType()),
                         StructField('carrera', StringType()),
                         ])

dataframe = spark.read.csv("curso.csv",
                           schema=csv_schema,
                           header=False)

dataframe.show()

#Lee nota.csv
csv_schema = StructType([StructField('numeroCarnet', IntegerType()),
                         StructField('codigoCurso', StringType()),
                         StructField('nota', StringType()),
                         ])

dataframe = spark.read.csv("nota.csv",
                           schema=csv_schema,
                           header=False)

dataframe.show()