
from readestudiante import read
from joinerestudiante import joinDataSets
from aggregateestudiante import aggregateFunction

# Programa Principal

estudianteFrame, notaFrame, cursoFrame = read() # Lectura de CSVs
joint_df = joinDataSets(estudianteFrame, notaFrame, cursoFrame) #Join de datasets
aggData = aggregateFunction(joint_df) #Funcion aggregate

aggData.show()






