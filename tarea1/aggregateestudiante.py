from pyspark.sql.functions import col

def aggregateFunction(joint_dataset):
    sumaCreditos_df = joint_dataset.groupBy("numeroCarnet").sum("creditos", "nota * creditos")
    sumaCreditos_df= sumaCreditos_df.withColumn("ponderados por creditos", col("sum(nota * creditos)") / col("sum(creditos)"))
    return sumaCreditos_df