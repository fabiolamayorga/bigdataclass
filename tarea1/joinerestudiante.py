from pyspark.sql.functions import col



def joinDataSets(estudiante_df, nota_df, curso_df):
    joint_en = estudiante_df.join(nota_df, estudiante_df.numeroCarnet == nota_df.idCarnet, "left") #join de estudiantes y notas
    joint_total = joint_en.join(curso_df, joint_en.codigoCurso == curso_df.codigoCurso, "left")
    notaCreditos_df = joint_total.withColumn("nota * creditos", col("nota") * col("creditos"))
    return notaCreditos_df