from pyspark.sql.functions import col

def sort_estudiante_by_notes(dataset, cantidad):
    return dataset.orderBy(col('ponderados por creditos'), ascending=False).limit(cantidad)