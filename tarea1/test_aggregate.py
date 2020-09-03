from .aggregateestudiante import aggregateFunction
from .joinerestudiante import joinDataSets
from pyspark.sql import SparkSession


def test_aggregate(spark_session):
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
        (3421510, 2,	'Enfermeria'), 
        (6262158, 2,	'Informatica'), 
        (3825359,8,'Biotecnologia'), 
        (4117669,8,'Filologia')
    ]
    cursos_ds = spark_session.createDataFrame(cursos_data,
                                               ['codigoCurso', 'creditos','carrera'])

    joint_ds = joinDataSets(estudiante_ds, notas_ds, cursos_ds);
    actual_ds = aggregateFunction(joint_ds)

    joint_ds.show()
    actual_ds.show()

    expected_ds = spark_session.createDataFrame(
        [
            (6520985, 2, 13.6, 6.8),
            (6626704, 8, 45.6, 5.7),
        ],
        ['numeroCarnet', 'sum(creditos)', 'sum(nota * creditos)', 'ponderados por creditos'])
    
    expected_ds.show()
    # actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()



#test con un estudiante repetido
def test_aggregate_estudiante_repetido(spark_session):
    notas_data = [
        (6626704, 4117669, 5.7),
        (6626704, 3421510, 7.0), 
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
        (3421510, 2,	'Enfermeria'), 
        (6262158, 2,	'Informatica'), 
        (3825359,8,'Biotecnologia'), 
        (4117669,8,'Filologia')
    ]
    cursos_ds = spark_session.createDataFrame(cursos_data,
                                               ['codigoCurso', 'creditos','carrera'])

    joint_ds = joinDataSets(estudiante_ds, notas_ds, cursos_ds);
    actual_ds = aggregateFunction(joint_ds)

    joint_ds.show()
    actual_ds.show()

    expected_ds = spark_session.createDataFrame(
        [
            (6520985, 2, 13.6, 6.8),
            (6626704, 10, 59.6, 5.96),
        ],
        ['numeroCarnet', 'sum(creditos)', 'sum(nota * creditos)', 'ponderados por creditos'])
    
    #expected_ds.show()
    # actual_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
