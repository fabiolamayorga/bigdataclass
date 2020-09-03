from .readestudiante import read
from .joinerestudiante import joinDataSets
from pyspark.sql import SparkSession

#Funcion para comprobar que el join esta funcionando y retorna la estructura adecuada
def test_join(spark_session):
    notas_data = [(6626704, 4117669, 5.7), (6520985, 3421510, 6.8)]
    notas_ds = spark_session.createDataFrame(notas_data,
                                              ['idCarnet', 'codigoCurso', 'nota'])
    estudiante_data = [
        (6626704, 'Cordelia Shucksmith', 'Enfermeria'), 
        (6520985, 'Jana Barhems', 'Biotecnologia')
    ]
    estudiante_ds = spark_session.createDataFrame(estudiante_data,
                                               ['numeroCarnet', 'nombreCompleto','carrera'])

    cursos_data = [
        (3421510, 2,'Enfermeria'), 
        (6262158, 2,'Informatica'), 
        (3825359,8,'Biotecnologia'), 
        (4117669,8,'Filologia')]
    cursos_ds = spark_session.createDataFrame(cursos_data,
                                               ['codigoCurso', 'creditos','carrera'])

    notas_ds.show()
    estudiante_ds.show()
    cursos_ds.show()

    actual_ds = joinDataSets(estudiante_ds, notas_ds, cursos_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (6520985, 'Jana Barhems', 'Biotecnologia', 6520985, 3421510, 6.8, 3421510, 2, 'Enfermeria',  13.6),
            (6626704,'Cordelia Shucksmith', 'Enfermeria', 6626704, 4117669, 5.7, 4117669, 8, 'Filologia', 45.6,),
        ],
        ['numeroCarnet', 'nombreCompleto', 'carrera', 'idCarnet', 'codigoCurso', 'nota', 'codigoCurso', 'creditos', 'carrera', 'nota * creditos'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

#Este test este hecho para fallar, no hay carrera asociada al curso   
def test_join_no_carrer(spark_session):
    notas_data = [
        (6626704, 4117669, 5.7), 
        (6520985, 3421510, 6.8)
    ]
    notas_ds = spark_session.createDataFrame(notas_data,
                                              ['idCarnet', 'codigoCurso', 'nota'])
    estudiante_data = [
        (6626704, 'Cordelia Shucksmith', 'Enfermeria'), 
        (6520985, 'Jana Barhems', 'Biotecnologia')
    
    ]
    estudiante_ds = spark_session.createDataFrame(estudiante_data,
                                               ['numeroCarnet', 'nombreCompleto','carrera'])

    cursos_data = [
        (3421510, 2,'Enfermeria'), 
        (6262158, 2,'Informatica')]
    cursos_ds = spark_session.createDataFrame(cursos_data,
                                               ['codigoCurso', 'creditos','carrera'])

    notas_ds.show()
    estudiante_ds.show()
    cursos_ds.show()

    actual_ds = joinDataSets(estudiante_ds, notas_ds, cursos_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (6520985, 'Jana Barhems', 'Biotecnologia', 6520985, 3421510, 6.8, 3421510, 2, 'Enfermeria',  13.6),
            (6626704,'Cordelia Shucksmith', 'Enfermeria', 6626704, 4117669, 5.7, 4117669, 8, 'Filologia', 45.6,),
        ],
        ['numeroCarnet', 'nombreCompleto', 'carrera', 'idCarnet', 'codigoCurso', 'nota', 'codigoCurso', 'creditos', 'carrera', 'nota * creditos'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

