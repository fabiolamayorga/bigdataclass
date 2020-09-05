
from readestudiante import read
from joinerestudiante import joinDataSets
from aggregateestudiante import aggregateFunction
from sortestudiante import sort_estudiante_by_notes
import sys 

# Programa Principal

files_names = sys.argv[1:]
print('files name', files_names)
estudianteFrame, notaFrame, cursoFrame = read(files_names[0], files_names[1], files_names[2]) # Lectura de CSVs
joint_df = joinDataSets(estudianteFrame, notaFrame, cursoFrame) #Join de datasets
aggData = aggregateFunction(joint_df) #Funcion aggregate
aggData.show()

sorted_result = sort_estudiante_by_notes(aggData, 10)

sorted_result.show()






