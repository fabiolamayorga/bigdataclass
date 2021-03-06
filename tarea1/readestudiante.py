from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (IntegerType, FloatType, StructField,
                               StructType, StringType)
                               
def read(estudianteCSV, cursoCSV, notasCSV):
    spark = SparkSession.builder.appName("Read Data of Homework").getOrCreate()


    ## Lee estudiante.csv
    csv_schema = StructType([StructField('numeroCarnet', IntegerType()),
                            StructField('nombreCompleto', StringType()),
                            StructField('carrera', StringType()),
                            ])

    estudianteFrame = spark.read.csv(estudianteCSV,
                            schema=csv_schema,
                            header=False)

    #estudianteFrame.show()


    #Lee curso.csv
    csv_schema = StructType([StructField('codigoCurso', IntegerType()),
                            StructField('creditos', IntegerType()),
                            StructField('carrera', StringType()),
                            ])

    cursoFrame = spark.read.csv(cursoCSV,
                            schema=csv_schema,
                            header=False)

    #cursoFrame.show()

    #Lee nota.csv
    csv_schema = StructType([StructField('idCarnet', IntegerType()),
                            StructField('codigoCurso', StringType()),
                            StructField('nota', FloatType()),
                            ])

    notaFrame = spark.read.csv(notasCSV,
                            schema=csv_schema,
                            header=False)

    #notaFrame.show()
    return estudianteFrame, notaFrame, cursoFrame
